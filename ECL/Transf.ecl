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

// Here's a routine that uses the shared object from Init.
// Notice that it must receive handle even though it's not used.
// Otherwise, we can't guarantee that fmInit will be called first.
knn_rec:=RECORD
    INTEGER4 SI ;
    INTEGER4 KNNSI;
    INTEGER4 knn;
    REAL4 dis;
    REAL4 Kdis:=0;
    REAL KNNdis:=0;
    REAL LDR;
    REAL4 LOF:=0;
   
END;

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

knn_rec2:= RECORD
    INTEGER SI;
    REAL4 KDIS;
END;

STREAMED DATASET(knn_rec2) kdistance(STREAMED DATASET(knn_rec) recs,UNSIGNED handle ) :=
           EMBED(Python: globalscope('facScope'), persist('query'), activity)
    
    
    for x in range(0,len(OBJECT.kdis)):
       yield(OBJECT.kdis[x][0])
        

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

MyDS2:=DISTRIBUTE(firstDS, SI);                            ////////////////////////////////////////IMPORTANT 
OUTPUT(MyDS2, NAMED('MyDS2'));
INTEGER K:=5;
MyDS3 := knn(MyDS2, handle, K);
OUTPUT(MyDS3, NAMED('MyDS3'));

// knn_rec3:=RECORD
//     knn_rec.SI;
//     knn_rec.knn;
//     knn_rec.dis;
//     knn_rec.reach;
//     knn_rec.LRD;
// END;
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

//d1:=dataset(MyDS3, knn_rec);

withKdis:= JOIN(MyDS3,
                firstDS,
                LEFT.KNN=RIGHT.SI and LEFT.KNNSI=0,
                JoinThem(LEFT));
OUTPUT(withKdis, NAMED('withKdis'));

knn_rec4:=RECORD
   INTEGER4 SI:=0;
   INTEGER4 upper:=0;
   REAL4 reach:=0;
END;

knn_rec JoinThem2(MyDS3 L) := TRANSFORM
   SELF.SI:=L.si;
   //SELF.upper:=L.SI;
   SELF.KNNdis:= MAX(L.dis,withKdis[L.knn+1].KDIS2);
   SELF:=l;

END;

reach:= PROJECT(MyDS3,JoinThem2(LEFT));
OUTPUT(reach, NAMED('reach'));


knn_rec RollThem(reach L, reach R) := TRANSFORM
    SELF.ldr := IF(R.dis=0, 0, R.knndis +L.ldr);
    SELF := L; 
END;
//USING ROLLUP TO SUM THE REACHABILITY DISTANCE
 RolledUpRecs := ROLLUP( reach, 
                        LEFT.SI = RIGHT.SI, 
                        RollThem(LEFT, RIGHT));

OUTPUT( RolledUpRecs , NAMED('RolledUpRecs'));

knn_rec3 DeNormThem(withKdis L, RolledUpRecs R) := TRANSFORM
    SELF.LRD:= (K-1)/R.ldr;
    SELF := L;
END;

lrd_included := DENORMALIZE(withKdis, RolledUpRecs, 
                            LEFT.SI = RIGHT.SI, 
                            DeNormThem(LEFT, RIGHT));
OUTPUT(lrd_included , NAMED('DeNormedRecs'));



knn_rec project_dis(reach l) :=TRANSFORM
    SELF.LDR:= LRD_INCLUDED[l.knn+1].lrd;
    self:= l;
END;

final:=project( reach, project_dis(LEFT));
OUTPUT(final, NAMED('final'));

knn_rec add_lof(final L, final R) := TRANSFORM
    SELF.lof := IF(R.dis=0, 0, R.ldr +L.lof);
    SELF := L; 
END;
//USING ROLLUP TO SUM THE REACHABILITY DISTANCE
 LOF := ROLLUP( final, 
                    LEFT.SI = RIGHT.SI, 
                        add_lof(LEFT, RIGHT));

OUTPUT( LOF , NAMED('LOF'));

knn_rec3 FINAL_TOUCH(lrd_included L, RolledUpRecs R) := TRANSFORM
    SELF.LOF:= R.LOF/(L.LRD* (K-1));
    SELF := L;
END;

lOF_included := DENORMALIZE(lrd_included, LOF, 
                            LEFT.SI = RIGHT.SI, 
                            FINAL_TOUCH(LEFT, RIGHT));
OUTPUT(lOF_included , NAMED('lOF_included'));


