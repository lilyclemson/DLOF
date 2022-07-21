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
// STREAMED DATASET(handleRec) fmInit(STREAMED DATASET(dummy_rec) recs) :=
//            EMBED(Python: globalscope('facScope'), persist('query'), activity)
  
//     global OBJECT
//     #, OBJ_NUM
    
//     import numpy 
//     from sklearn.neighbors import KDTree

    
//     if 'OBJECT' not in globals():
//         class kdCreate:
//             def __init__(self,points):
//                 self.base=0
//                 self.tree=KDTree(points)
//             def storebase(self, base):
//                 self.base=base
                
        
//         points=[] 
//         for recTuple in recs:
//             interList=list(recTuple[1:])
//             interList= list(map(float,interList))
//             points.append(interList)

   
//         OBJECT =kdCreate(points)
        

//     # We return a single dummy record with the object handle inside.
//     return[(1,)]
// ENDEMBED;

// Here's a routine that uses the shared object from Init.
// Notice that it must receive handle even though it's not used.
// Otherwise, we can't guarantee that fmInit will be called first.
// knn_rec:=RECORD
//     INTEGER4 SI ;
//     INTEGER4 knn;
//     REAL4 dis;
//     REAL4 reach:=0;
//     REAL4 LRD:=0;
// END;

// STREAMED DATASET(knn_rec) knn(STREAMED DATASET(dummy_rec) recs, UNSIGNED handle, INTEGER K) :=
//            EMBED(Python: globalscope('facScope'), persist('query'), activity)
    
//     offset=0
//     i=0
//     for recTuple in recs:
//         if(i==0):
//             OBJECT.storebase(int(recTuple[0]))
//         if(i==1):
//             offset=OBJECT.base
//         searchItem=list(recTuple[1:])
//         searchItem=list(map(float,searchItem))
//         dis, ind=OBJECT.tree.query([list(searchItem)], K)
        
//         for x in range(0, len(dis[0])):
//             result=(int(recTuple[0]),int(ind[0][x]+offset),float(dis[0][x]),float(0),float(0))
//             yield (result)
        
//         i=i+1
// ENDEMBED;
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

setindex:= record
  integer4 num;
  end;
setdis:= record
  real4 dis;
  end;
knn_rec:=RECORD
    INTEGER4 SI ;
    DATASET(setindex) knns;
    DATASET(setdis) kdis;

END;


STREAMED DATASET(knn_rec) knn(STREAMED DATASET(dummy_rec) recs, UNSIGNED handle, INTEGER K) :=
           EMBED(Python: globalscope('facScope'), persist('query'), activity)
    
    to_add=0
    to_add2=0
    i=0
    for recTuple in recs:
        if(i==0):
            to_add=int(recTuple[0])
        if(i==1):
            to_add2=to_add
        
        searchItem=list(recTuple[1:])
        searchItem=list(map(float,searchItem))
        dis, ind=OBJECT.query([list(searchItem)], K)
        kindex=list(map(int,ind[0]+to_add2))
        kdis=list(map(float,dis[0]))
        yield (recTuple[0],kindex, kdis)
        i+=1
ENDEMBED;

dummy_rec addSI(anomaly L, INTEGER C) := TRANSFORM
    SELF.SI:=C-1 ;
    SELF := L;
END;

firstDS:= PROJECT(anomaly, addSI(LEFT, COUNTER));
MyDS := DISTRIBUTE(firstDS, SI);
OUTPUT(MyDS, NAMED('MyDS'));

handles:=fmInit(MyDS);

OUTPUT(handles, NAMED('handles'));

handle:=MIN(handles,handle);
OUTPUT(handle, NAMED('handle'));

MyDS2:=DISTRIBUTE(firstDS, ALL);
OUTPUT(MyDS2, NAMED('MyDS2'));
INTEGER K:=5;
MyDS3 := knn(MyDS2, handle, K);

 OUTPUT(MyDS3, NAMED('Dataset_Complete'));
 MyDS4:=SORT(MyDS3,SI);
 OUTPUT(MyDS4);
// OUTPUT(COUNT(MyDS3));
// RT_folk := JOIN(MyDS3,
//                 MyDS3,
//                 LEFT.=RIGHT.id,
//                 TRANSFORM(outrec,SELF := LEFT));
// OUTPUT(RT_folk);

//OUTPUT(MyDS3,,'~CLASS::ARA::OUT::LOCALKNN', overwrite);
//GroupedRecs1 := GROUP(MyDS3, SI);


// RT_folk := JOIN(MyDS3,
//                 MyDS3,
//                 LEFT.id=RIGHT.id,
//                 TRANSFORM(outrec,SELF := LEFT));
// OUTPUT(RT_folk);
