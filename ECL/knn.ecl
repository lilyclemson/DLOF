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

        points=[] 
        for recTuple in recs:
            interList=list(recTuple[1:])
            interList= list(map(float,interList))
            points.append(interList)

    # Now instantiate the object that we want to use repeatedly
        OBJECT =KDTree(points)
        

    # We return a single dummy record with the object handle inside.
    return[(1,)]
ENDEMBED;

// Here's a routine that uses the shared object from Init.
// Notice that it must receive handle even though it's not used.
// Otherwise, we can't guarantee that fmInit will be called first.
knn_rec:=RECORD
    INTEGER4 SI ;
    INTEGER4 knn;
    REAL4 dis;
    REAL4 reach:=0;
    REAL4 LRD:=0;
END;


// ENDEMBED;
parentrec:=RECORD
    INTEGER4 SI;
    REAL4 Kdis;
END;
setindex:= record
  integer4 num;
  end;
setdis:= record
  real4 dis;
  end;
knn_rec2:=RECORD
    parentrec;
    DATASET(setindex) knns;
    DATASET(setdis) dis; 
END;

STREAMED DATASET(knn_rec2) knn(STREAMED DATASET(dummy_rec) recs, UNSIGNED handle, INTEGER K) :=
           EMBED(Python: globalscope('facScope'), persist('query'), activity)
    
    
    for recTuple in recs:
        searchItem=list(recTuple[1:])
        searchItem=list(map(float,searchItem))
        dis, ind=OBJECT.query([list(searchItem)], K)
        kindex=list(map(int,ind[0]))
        kdis=list(map(float,dis[0]))
        yield (recTuple[0], dis[0][K-1],kindex, kdis)
ENDEMBED;

dummy_rec addSI(anomaly L, INTEGER C) := TRANSFORM
    SELF.SI:= C-1;
    SELF := L;
END;

firstDS:= PROJECT(anomaly, addSI(LEFT, COUNTER));
MyDS := DISTRIBUTE(firstDS, ALL);
OUTPUT(MyDS, NAMED('InputDataset'));

handles:=fmInit(MyDS);
OUTPUT(handles, NAMED('handles'));
handle:=MIN(handles,handle);
OUTPUT(handle, NAMED('handle'));

MyDS2:=DISTRIBUTE(firstDS);                                 ////////////////////////////////////////IMPORTANT 
OUTPUT(MyDS2, NAMED('MyDS2'));
INTEGER K:=5;
MyDS3 := knn(MyDS2, handle, K);
OUTPUT(MyDS3, NAMED('MyDS3'));

DisMyDS3:=distribute(MyDS3);

parentrec parent_out (MyDS3 L):=TRANSFORM
    SELF := L;
END;

POUT:= PROJECT(MyDS3,parent_out(LEFT));

OUTPUT(POUT, NAMED('KDIS'));
setindex child_out (MyDS3 L, INTEGER C):=TRANSFORM
    SELF := L.knns[C];
END;
Pchild:=NORMALIZE(MyDS3, LEFT.SI, child_out(LEFT, COUNTER));
OUTPUT(Pchild, NAMED('CHILD'));

knn_rec5:=RECORD
    INTEGER4 SI ;
    INTEGER4 KNNSI;
    INTEGER4 knn;
    REAL4 dis;
    REAL4 Kdis:=0;
    REAL KNNdis:=0;
   
END;

STREAMED DATASET(knn_rec5) knn2(STREAMED DATASET(dummy_rec) recs, UNSIGNED handle, INTEGER K) :=
           EMBED(Python: globalscope('facScope'), persist('query'), activity)
    
    
    for recTuple in recs:
        searchItem=list(recTuple[1:])
        searchItem=list(map(float,searchItem))
        dis, ind=OBJECT.tree.query([list(searchItem)], K)
        for x in range(0, len(dis[0])):
            
            result=(int(recTuple[0]),int(x),int(ind[0][x]),float(dis[0][x]),float(dis[0][K-1]), float(0))
            
            yield (result)
      
ENDEMBED;

MyDS3_copy := knn2(MyDS2, handle, K);
OUTPUT(MyDS3_copy, NAMED('MyDS3_copy'));

KNN_REC6 := RECORD
     INTEGER SI;
     REAL4 REACH;
     INTEGER KNN;
END;
knn_rec6 JoinThem(MyDS3_copy L, POUT R) := TRANSFORM
   SELF.SI:=L.SI;
   SELF.reach:=R.kdis;
   SELF.KNN:=L.knn;    
END;

withKdis:= JOIN(MyDS3_copy,
                POUT,
                LEFT.KNN=RIGHT.SI,
                JoinThem(LEFT, RIGHT));
OUTPUT(withKdis, NAMED('withKdis'));

// knn_rec2 JoinThem(MyRec L, MyRec R) := TRANSFORM
//     SELF.Value1 := IF(L.Value1<>'', L.Value1, R.Value1);
//     SELF.LeftValue2 := L.Value2;
//     SELF.RightValue2 := R.Value2;
// END;

// InnerJoinedRecs := JOIN(LeftFile, RightFile, 
//                         LEFT.Value1 = RIGHT.Value1, 
//                         JoinThem(LEFT, RIGHT));

// MyOutRec:=RECORD
//     INTEGER4 SI;
//     REAL4 KDIS;
// END;
// MyOutRec JoinThem(DisMyDS3 L, firstDS R) := TRANSFORM
//     SELF.SI := R.SI;
//     SELF. KDIS:= SUM(L.dis.dis); 
// END;

// InnerJoinedRecs := JOIN(DisMyDS3, firstDS, 
//                         LEFT.SI=RIGHT.SI, 
//                         JoinThem(LEFT, RIGHT));

// OUTPUT(InnerJoinedRecs, NAMED('InnerJoinedRecs'));
