from sklearn.neighbors import NearestNeighbors
import numpy as np
samples=Input_data = np.array([[-1, 1], [-2, 2], [-3, 3], [1, 2], [2, 3], [3, 4],[4, 5]])
class knn:
    nn= NearestNeighbors(n_neighbors=5, algorithm="kd_tree")
    nn.fit(samples)
    dis, points=nn.kneighbors(nn)
    print(dis)
    

