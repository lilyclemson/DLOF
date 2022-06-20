IMPORT Python3 AS Python;
anomaly:= $.File_dlof.File;
anomalyLay:=$.File_dlof.Layout;

handleRec := RECORD
  UNSIGNED handle;
END;
dummy_rec:=RECORD  
  INTEGER4 SI;
  anomalyLay;
END;

// Function to initialise KD tree locally on each node
STREAMED DATASET(handleRec) fmInit(STREAMED DATASET(dummy_rec) recs) :=
           EMBED(Python: globalscope('facScope'), persist('query'), activity)
  
    global OBJECT
    #, OBJ_NUM
    
    import numpy 
    from sklearn.neighbors import KDTree

    if 'OBJECT' not in globals():
        # This is your one-time initializer code.  It will only be executed once on each node.
        # All global initialization goes here.
       #UNPACK
        class kdCreate:
            def __init__(self,points):
                self.kdis=[]
                self.tree=KDTree(points)
            def storeKdis(self, ind, dis):
                self.kdis.append((ind,dis))
                
        
        points=[] 
        for recTuple in recs:
            interList=list(recTuple[1:])
            interList= list(map(float,interList))
            points.append(interList)

    # Now instantiate the object that we want to use repeatedly
        OBJECT =kdCreate(points)
        

    # We return a single dummy record with the object handle inside.
    return[(1,)]
ENDEMBED;

// Record to store points its KNNs, its K distance, K distance of KNNs, LRD of its KNNs. 
knn_rec:=RECORD
    INTEGER4 SI ;
    INTEGER4 KNNSI;
    INTEGER4 knn;
    REAL4 dis;
    REAL4 Kdis:=0;
    REAL KNNdis:=0;
    REAL LRD;
    REAL4 LOF:=0;
   
END;

//Function to query the created KD tree on each node

STREAMED DATASET(knn_rec) knn(STREAMED DATASET(dummy_rec) recs, UNSIGNED handle, INTEGER K) :=
           EMBED(Python: globalscope('facScope'), persist('query'), activity)
    
    
    for recTuple in recs:
        searchItem=list(recTuple[1:])
        searchItem=list(map(float,searchItem))
        dis, ind=OBJECT.tree.query([list(searchItem)], K)
        
        for x in range(0, len(dis[0])):
            result=(int(recTuple[0]),int(x),int(ind[0][x]),float(dis[0][x]),float(dis[0][K-1]), float(0), float(0),float(0))
            yield (result)
        OBJECT.kdis.append((int(recTuple[0])))
ENDEMBED;

// 0  based numbering for SI (Index)
dummy_rec addSI(anomaly L, INTEGER C) := TRANSFORM
    SELF.SI:= C-1;
    SELF := L;
END;

firstDS:= PROJECT(anomaly, addSI(LEFT, COUNTER));

//Build global tree on each node hence distribute same the whole dataset to each node
MyDS := DISTRIBUTE(firstDS, ALL);
OUTPUT(MyDS, NAMED('InputDataset'));
handles:=fmInit(MyDS);
OUTPUT(handles, NAMED('handles'));
handle:=MIN(handles,handle);
OUTPUT(handle, NAMED('handle'));

// Actual K is one less then k initialised here. KNN returns the point itself as neighbor 
//  If user input = m then for this program input K=m+1
//C is the contamination or user estimate of outliers in dataset
INTEGER K:=5;
INTEGER C:=150000;

//Query the KD tree for each point, distribute the load across all nodes
//Each node gets unique points, querying will give actual KNNs of those point
MyDS2:=DISTRIBUTE(firstDS, SI);                             
OUTPUT(MyDS2, NAMED('MyDS2'));
MyDS3 := knn(MyDS2, handle, K);
OUTPUT(MyDS3, NAMED('MyDS3'));
OUTPUT(COUNT(MyDS2));


knn_rec3:=RECORD
   INTEGER4 SI:=0;
   REAL4 KDIS2:=0;
   REAL4 LRD:=0;
   REAL4 LOF:=0;
END;
knn_rec3 JoinThem(MyDS3 L) := TRANSFORM
   SELF.SI:=l.SI;
   SELF.KDIS2:=L.kdis;   
END;

// To get K distance of all points and store in ascending order of Index(SI) 
withKdis:= JOIN(MyDS3,
                firstDS,
                LEFT.KNN=RIGHT.SI and LEFT.KNNSI=0,
                JoinThem(LEFT));
OUTPUT(withKdis, NAMED('withKdis'));


//Transform to get the K distance of KNNs of p

knn_rec JoinThem2(MyDS3 L) := TRANSFORM
   SELF.SI:=L.si;
   SELF.KNNdis:= MAX(L.dis,withKdis[L.knn+1].KDIS2);
   SELF:=l;

END;
// Here we store reachability ditance of p wrt to all its KNNs
reach:= PROJECT(MyDS3,JoinThem2(LEFT));
OUTPUT(reach, NAMED('reach'));

//USING ROLLUP TO SUM THE REACHABILITY DISTANCE of p wrt all its KNNs
knn_rec RollThem(reach L, reach R) := TRANSFORM
    SELF.lrd := IF(R.dis=0, 0, R.knndis +L.lrd);
    SELF := L; 
END;


 withRD := ROLLUP( reach, 
                        LEFT.SI = RIGHT.SI, 
                        RollThem(LEFT, RIGHT));

OUTPUT( withRD , NAMED('withRD'));

// Finding LRD of each point by dividing K with sum of reach distance wrt KNNs
knn_rec3 DeNormThem(withKdis L, withRD R) := TRANSFORM
    SELF.LRD:= (K-1)/R.lrd;
    SELF := L;
END;
// lrd_included contains LRD of all points in sorted order of Index(SI)
lrd_included := DENORMALIZE(withKdis, withRD, 
                            LEFT.SI = RIGHT.SI, 
                            DeNormThem(LEFT, RIGHT));
OUTPUT(lrd_included , NAMED('LRDincluded'));


// Access the lrd in the lrd_included and store beside each KNN of p
knn_rec project_dis(reach l) :=TRANSFORM
    SELF.Lrd:= LRD_INCLUDED[l.knn+1].lrd;
    self:= l;
END;

final:=project( reach, project_dis(LEFT));
OUTPUT(final, NAMED('final'));

knn_rec add_lof(final L, final R) := TRANSFORM
    SELF.lof := IF(R.dis=0, 0, R.lrd +L.lof);
    SELF := L; 
END;
//Using Rollup to find sum of LRD of all KNNs of every point in dataset
 LOF := ROLLUP( final, 
                    LEFT.SI = RIGHT.SI, 
                        add_lof(LEFT, RIGHT), PARALLEL);

OUTPUT( LOF , NAMED('LOF'));

// Fill LOF of every 
knn_rec3 fill_lof(lrd_included L, LOF R) := TRANSFORM
    SELF.LOF:= R.LOF/(L.LRD* (K-1));
    SELF := L;
END;
// Enter LOF of every point in sorted order of SI (Index)
 LOF_included:= DENORMALIZE(lrd_included, LOF, 
                            LEFT.SI = RIGHT.SI, 
                            fill_lof(LEFT, RIGHT));
OUTPUT( LOF_included , NAMED('lOF_included'));

//Sort to find the N most outlying points
lofsort:=SORT( LOF_included, -LOF);
OUTPUT(lofsort, NAMED('LOFsorted'));

//Threshold is the boundary between user required Outliers and and Inliers
REAL4 thresh:=lofsort[C].LOF;

testRec:=RECORD
INTEGER4 SI;
REAL4 LOFval;  
BOOLEAN boolLOF;
END;

// Check LOF of every value with threshhold
// if LOF> Threshold, outlier else inlier
testRec fillLOF(LOF_included L):=TRANSFORM

   SELF.boolLOF:=if(L.LOF>=thresh, true, false);
   SELF.LOFval:=L.LOF;
   SELF.SI:=L.SI;
END;
//Result with LOF value and boolean LOF value
FitPredict:= PROJECT(LOF_included, fillLOF(LEFT));
OUTPUT(FitPredict,NAMED('FitPredict'));