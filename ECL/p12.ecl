IMPORT Python3 AS Python;
anomalyLay:=RECORD
  INTEGER a;
  INTEGER b;
END;
anomaly:= DATASET([{126,173},
{131,115},
{141,175},
{230,160},
{231,146},
{231,147},
{231,182},
{232,165},
{232,164},
{340,172},
{341,136},
{342,166}
], anomalyLay);

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
INTEGER K:=3;
INTEGER C:=150000;

//Query the KD tree for each point, distribute the load across all nodes
//Each node gets unique points, querying will give actual KNNs of those point
MyDS2:=DISTRIBUTE(firstDS, SI);                             
OUTPUT(MyDS2, NAMED('MyDS2'));
MyDS3 := knn(MyDS2, handle, K);
OUTPUT(MyDS3, NAMED('MyDS3'));

MyDS4:=SORT(MyDS3(dis=0), SI);
OUTPUT(MyDs4, NAMED('MyDS4'));

 knn_rec3:=RECORD
    INTEGER4 SI:=0;
    INTEGER4 KNN:=0;
    REAL4 dis:=0;
    REAL4 knnKdis:=0;
 END;

knn_rec3 JoinThem(MyDS3 L, MyDS4 R) := TRANSFORM
   SELF.SI:=L.SI;
   SELF.KNN:=L.KNN;
   SELF.dis:=L.dis;
   SELF.KnnKdis:=R.kdis;   
END;

withKdis:= JOIN(MyDS3,
                MyDS4,
                LEFT.KNN=RIGHT.SI,
                JoinThem(LEFT, RIGHT));
OUTPUT(SORT(withKdis, si), NAMED('withKdis'));

reachD := TABLE(withKdis, {si, reachDist := K/SUM(GROUP, knnKdis)}, si);
OUTPUT(reachD, NAMED('reachD'));
