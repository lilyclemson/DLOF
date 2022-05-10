from sklearn.neighbors import NearestNeighbors
import numpy as np

samples= np.array([[-1, 1], [-2, 2], [-3, 3], [1, 2], [2, 3], [3, 4],[4, 5]]) 
def knn(samples,k=3):
   
    nn= NearestNeighbors(n_neighbors=k, algorithm="kd_tree")
    nn.fit(samples)
    dis, points=nn.kneighbors(samples)
    print(dis)
samples= np.array([[-1, 1], [-2, 2], [-3, 3], [1, 2], [2, 3], [3, 4],[4, 5]]) 
out=knn(samples,6);

