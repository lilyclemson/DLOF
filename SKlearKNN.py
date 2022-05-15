
from sklearn.neighbors import NearestNeighbors
import numpy as np

class sklearnKNN:
    def find_knns(self):
        nn= NearestNeighbors(n_neighbors=5 )
        nn.fit(X)
        dis, points=nn.kneighbors(X)
        combined=zip(dis,points)
        return combined