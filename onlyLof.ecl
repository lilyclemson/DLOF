// ECL program to implement LOF for anomaly detection
// Dataset: minids (5 fields, 3000 numerical records)
IMPORT Python3 AS Python;
IMPORT Std.System.Thorlib;
IMPORT STD;
// anomaly:= $.minids.mini_file;
// anomalyLay:=$.minids.mini_lay;

//  anomaly:= $.File_time.File;
//  anomalyLay:=$.File_time.Layout;

// IMPORT Python3 AS Python;
anomaly:= $.File_test.File_test1;
anomalyLay:=$.File_test.Lay;


// Actual K is one less then k initialised here. sklearn KNN returns the point itself as one of the nearest neighbor 
//  If user input = m then for this program input K=m+1
// C is the contamination or user estimate of percentage of outliers in dataset
INTEGER K:=900;
REAL con:=0.05;
INTEGER C:=con* COUNT(anomaly);


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
                self.points=points
                l=[]
                for x in range(0, len(points)):
                    l.append(points[x][1:])
                self.tree=KDTree(l)
                
        
        points=[] 
        for recTuple in recs:
            interList=list(recTuple[:])
            interList= list(map(float,interList))
            points.append(interList)

    # Now instantiate the object that we want to use repeatedly
        OBJECT =kdCreate(points)
        

    # We return a single dummy record with the object handle inside.
    return[(1,)]
ENDEMBED;

// Record to store points its KNNs, its K distance, K distance of KNNs, LRD of its KNNs. 
knn_rec:=RECORD
    INTEGER4 SI ; //Index of record
    INTEGER4 knn; //Index of all KNNs of datapoint 
    INTEGER X;
    REAL4 dis; //Distance of datapoint with its knn
    REAL4 Kdis; //Store Kdistance of datapoint of index SI
   
END;

//Function to query the created KD tree on each node

STREAMED DATASET(knn_rec) knn(STREAMED DATASET(dummy_rec) recs, UNSIGNED handle, INTEGER K) :=
           EMBED(Python: globalscope('facScope'), persist('query'), activity)
    
    
    for recTuple in recs:
        searchItem=list(recTuple[1:])
        searchItem=list(map(float,searchItem))
        dis, ind=OBJECT.tree.query([list(searchItem)], K)
        
        for x in range(0, len(dis[0])):
            result=(int(recTuple[0]), int(OBJECT.points[ind[0][x]][0]), int(x) ,float(dis[0][x]),float(dis[0][K-1]))
            yield (result)

ENDEMBED;

// 0  based numbering for SI (Index)
dummy_rec addSI(anomaly L, INTEGER C) := TRANSFORM
    SELF.SI:= C-1;
    SELF := L;
END;

firstDS:= PROJECT(anomaly, addSI(LEFT, COUNTER));
//Build global tree on each node hence distribute same the whole dataset to each node
MyDS := DISTRIBUTE(firstDS, ALL);
//OUTPUT(MyDS, NAMED('InputDataset'));
handles:=fmInit(MyDS);
//OUTPUT(handles, NAMED('handles'));
handle:=MIN(handles,handle);
//OUTPUT(handle, NAMED('handle'));
//OUTPUT(Thorlib.Nodes());


//Query the KD tree for each point, distribute the load across all nodes
//Each node gets unique points, querying will give actual KNNs of those point
MyDS2:=DISTRIBUTE(firstDS, HASH(SI));
output(COUNT(MyDS2),NAMED('MYDS2'));                             
//OUTPUT(MyDS2, NAMED('distributed_dataset'));
MyDS3 := knn(MyDS2, handle, K);

// STD.File.DeSpray('class::ara::lof_no_dup::credit_no_duplicate.csv', 'localhost',
//         'class::ara::lof_no_dup::credit_no_duplicate2.csv');
output(COUNT(MyDS3),NAMED('MYDS3'));
//OUTPUT(MyDS3, NAMED('KNN_list'));

//MyDS4:=SORT(MyDS3(dis=0), SI); 
//Using sort will produce a list of points with Kdis sorted according to si 
MyDS4:=MyDS3(X=0);
output(COUNT(MyDS4),NAMED('MYDS4'));
//OUTPUT(SORT(MyDS4(SI>-1), SI),,'~thor::With_KDIS.csv',CSV, OVERWRITE);
OUTPUT(SORT(MyDs4,SI), NAMED('Kdis'));

dis_MyDS4:=DISTRIBUTE(MyDS4,ALL);
dis_MyDS3:=DISTRIBUTE(MyDS3,SI);
 knn_rec3:=RECORD
    INTEGER4 SI:=0;
    INTEGER4 KNN:=0;
    REAL4 dis:=0;
    INTEGER4 Knnjoin:=0; // Will store the SI of the right dataset joined
    REAL4 reach_dis:=0; // Store the Kdistance of Knn of point 
 END;

knn_rec3 JoinThem(dis_MyDS3 L, dis_MyDS4 R) := TRANSFORM
   SELF.SI:=L.SI;
   SELF.KNN:=L.KNN;
   SELF.dis:=L.dis;
   SELF.Knnjoin:=R.SI;
   SELF.reach_dis:=MAX(R.kdis, L.dis); //reachiblity distance  
END;
// Join to find the Kdistance of all Knns of point from MyDS4  
withKdis:= JOIN(dis_MyDS3,
                dis_MyDS4,
                LEFT.KNN=RIGHT.SI and LEFT.dis!=0, //Omit the point itself as Knn 
                JoinThem(LEFT, RIGHT), local);
// OUTPUT(SORT(withKdis(SI>-1), SI),,'~thor::kdis.csv',CSV, OVERWRITE);
output(COUNT(withKdis),NAMED('withKdis'));
// //OUTPUT(SORT(withKdis, si), NAMED('withKdis'));

///////////////////////////////////////////////////CHANGE////////////////////////////////////////////////
dis_withKdis:=DISTRIBUTE(withKdis, HASH(SI));
LRD_list := TABLE(dis_withKdis, {si, LRD := (K-1)/SUM(GROUP, reach_dis)}, si, LOCAL);
//local Table to sum the reachability distance and find lrd
// LRD_list := TABLE(withKdis, {si, LRD := (K-1)/SUM(GROUP, reach_dis)}, si);
output(COUNT(LRD_list),NAMED('LRD_list1'));
OUTPUT(SORTED(LRD_list, SI), NAMED('LRD_list'));

knn_rec4:=RECORD
    INTEGER4 SI:=0;
    INTEGER4 KNN:=0;
    INTEGER4 LRDjoin:=0;
    REAL4 LRD2:=0;
 END;

knn_rec4 add_LDR(MyDS3 L, LRD_list  R) := TRANSFORM
   SELF.SI:=L.SI;
   SELF.KNN:=L.KNN;
   SELF.LRDjoin:=R.SI;
   SELF.LRD2:=R.LRD;   
END;
// Join to populate the record for the LRDs of all KNNs of every point
LRDadded:= JOIN(MyDS3,
                LRD_list,
                LEFT.KNN=RIGHT.SI and LEFT.dis!=0,
                add_LDR(LEFT, RIGHT) );
output(COUNT(LRDadded),NAMED('LRDA'));
//sortedS:=Sort(LRDadded,si);
//OUTPUT(sortedS, named('LRDadded'), ALL);

dis_LRDadded:=DISTRIBUTE(LRDadded, HASH(SI));
LOF_list := TABLE(dis_LRDadded, {si, dummy_LOF := (SUM(GROUP, LRD2))/(K-1)}, si, LOCAL);
//LOF_list := TABLE(LRDadded, {si, dummy_LOF := (SUM(GROUP, LRD2))/(K-1)}, si);
output(COUNT(LOF_list),NAMED('LOF_list1'));
//OUTPUT(SORT(LOF_list(SI>-1), SI),,'~thor::final_lof.csv',CSV, OVERWRITE);
sort_LOF:=SORT(LOF_list(si>=0), si);


knn_rec5:=RECORD
    INTEGER4 SI;
    //INTEGER4 LRDjoin:=0;
    REAL4 LOF_val;
 END;

knn_rec5 add_LOF(sort_LOF L, LRD_list  R) := TRANSFORM
   SELF.SI:=L.SI;
   //SELF.LRDjoin:=R.SI;
   SELF.LOF_val:=L.dummy_LOF/R.LRD;   
END;
//join to divide LOF value of each datapoint in record with by LRD of same point 
LOF_final:= JOIN(sort_LOF,
                LRD_list,
                LEFT.SI=RIGHT.SI,
                add_LOF(LEFT, RIGHT));

OUTPUT(LOF_final, named('LOF_final'));
//LOF value of each point 
// Lof_display:=LOF_final(lof_val>1.5);             
// OUTPUT(COUNT(Lof_display));
lofTOPN:=TOPN(LOF_final,C,-LOF_val);
//OUTPUT(lofTOPN);
//Threshold is the boundary between user required Outliers and and Inliers
REAL4 thresh:=lofTOPN[C].LOF_val;

knn_rec6:=RECORD
INTEGER SI;
REAL LOFval;
BOOLEAN boolLOF;
END;

// Check LOF of every value with threshhold
// if LOF> Threshold, outlier else inlier
knn_rec6 isoutlier(LOF_final L):=TRANSFORM
   SELF.boolLOF:=if(L.LOF_val>=thresh, true, false);
   SELF.LOFval:=L.LOF_val;
   SELF.SI:=L.SI;
END;
//Result with LOF value and boolean LOF value
FitPredict:= PROJECT(LOF_final,  isoutlier(LEFT));
OUTPUT(FitPredict(boolLOF=true), NAMED('FitPredict'));

