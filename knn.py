from sklearn.neighbors import NearestNeighbors
import numpy as np

samples= np.array([[-1, 1], [-2, 2], [-3, 3], [1, 2], [2, 3], [3, 4],[4, 5]]) 
def knn(samples,k=3):
   
    nn= NearestNeighbors(n_neighbors=k, algorithm="kd_tree")
    nn.fit(samples)
    dis, points=nn.kneighbors(samples)
    print(dis)
    print(points)
# dis is n*k matrix and returns n=number of datapoints and k is the input which represents the distance of 
# k nearest neighbors in that point
samples= np.array([[-1, 1], [-2, 2], [-3, 3], [1, 2], [2, 3], [3, 4],[4, 5]]) 
out=knn(samples,6);
#always pass k+1 as parameter if k is number of nearest neighbors
'''
if k=5, the point itself is considered as a neighbour so we omit the first entry 
in the matrix dis
[[0.         1.41421356 2.23606798 2.82842712 3.60555128]
 [0.         1.41421356 1.41421356 3.         4.12310563]
 [0.         1.41421356 2.82842712 4.12310563 5.        ]
 [0.         1.41421356 2.23606798 2.82842712 3.        ]
 [0.         1.41421356 1.41421356 2.82842712 3.60555128]
 [0.         1.41421356 1.41421356 2.82842712 5.        ]
 [0.         1.41421356 2.82842712 4.24264069 6.40312424]]

'''



