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

STREAMED DATASET(knn_rec) knn(STREAMED DATASET(dummy_rec) recs, UNSIGNED handle, INTEGER K) :=
           EMBED(Python: globalscope('facScope'), persist('query'), activity)
    
    
    for recTuple in recs:
        searchItem=list(recTuple[1:])
        searchItem=list(map(float,searchItem))
        dis, ind=OBJECT.query([list(searchItem)], K)
        
        for x in range(0, len(dis[0])):
            result=[]
            result.append(int(recTuple[0]))
            result.append(int(ind[0][x]))
            result.append(float(dis[0][x]))
            result.append(float(0))
            result.append(float(0))
            yield (tuple(result))
ENDEMBED;

dummy_rec addSI(anomaly L, INTEGER C) := TRANSFORM
    SELF.SI:= C;
    SELF := L;
END;

firstDS:= PROJECT(anomaly, addSI(LEFT, COUNTER));
MyDS := DISTRIBUTE(firstDS, ALL);
OUTPUT(MyDS, NAMED('InputDataset'));

handles:=fmInit(MyDS);

OUTPUT(handles, NAMED('handles'));

handle:=MIN(handles,handle);
OUTPUT(handle, NAMED('handle'));

MyDS2:=DISTRIBUTE(MyDS);
OUTPUT(MyDS2, NAMED('MyDS2'));
INTEGER K:=10;
MyDS3 := knn(MyDS2, handle, K);
OUTPUT(MyDS3, NAMED('Dataset_Complete'));
MyDS4:=DISTRIBUTE(MyDS3);
knn_rec2 :=RECORD
INTEGER SI;
REAL KDIS;
END;

STREAMED DATASET(knn_rec2) kDistance(STREAMED DATASET(knn_rec) rec, INTEGER K) := EMBED(Python: activity)
    import numpy as np
    y=len(tuple(rec))
    recs = np.asarray(rec)
    empTuple=[0];
    for x in range(0,y, K):
        empTuple.clear()
        empTuple.append(int(recs[x][0]))
        empTuple.append(float(recs[x+K-1][2]))
        yield(tuple(empTuple))
ENDEMBED;

MyDS5:= kDistance(MyDS3,K);
OUTPUT(MyDS5, NAMED('KDIS'));
