
from sklearn.metrics import pairwise_distances 
class glof:
  def __init__(self,nodes,X, node_num,):
    self.nodes=nodes
    self.X=X
    self.total_sample=len(X)
    self.node_num=node_num
  def dlof_knn(self):
    neigh_num=5
    global_knn=[]
	 
    for k in range(0,self.total_sample):
      dummy=0
      knn_complete=[]
      for l in range(0, self.node_num):
        knn_inter_list=[]
        p=pairwise_distances(X[k].reshape(1, -1), nodes[l], metric='euclidean')
        if(k==0 and l==0):
          print(p.shape)
        for m in range (0,len(p[0])):
          list1=[]
          list1.append(p[0][m])
          list1.append(dummy+m)
          knn_inter_list.append(list1)
        knn_inter_list.sort()
        local_knn=[]
        for q in range(0,neigh_num):
          local_knn.append(knn_inter_list[q])
        knn_complete.extend(local_knn)
        dummy=dummy+len(p[0])
      knn_complete.sort()
      knn_complete=knn_complete[:neigh_num]
      global_knn.append(knn_complete)
      return global_knn
