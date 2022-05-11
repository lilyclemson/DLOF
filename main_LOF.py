
import pandas as pd
import numpy as np
from collections import Counter
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.neighbors import LocalOutlierFactor
from sklearn.datasets import make_classification
from sklearn.metrics import classification_report

total_sample=1000
X, Y = make_classification(n_samples=total_sample, n_features=5, n_informative=5,
                           n_redundant=0, n_repeated=0, n_classes=2,
                           n_clusters_per_class=1,
                           weights=[0.995, 0.005],
                           class_sep=0.5, random_state=0)

df = pd.DataFrame({'feature1': X[:, 0], 'feature2': X[:, 1],'feature3': X[:, 2],'feature4': X[:, 3],'feature5': X[:, 4],'target': Y})
count=0
for x in df.loc[:,"target"]:
  if x==1:
    count=count+1
outliers=count
normal=total_sample-count
print(f"Outliers = {count}")
print(f"normal = {total_sample-count}")


print(f"Nomrmals in  training dataset = {sorted(Counter(Y).items())[0][1]} \n outliers in  training dataset {sorted(Counter(Y).items())[1][1]}")

lof_outlier = LocalOutlierFactor(n_neighbors=5, novelty=False)
prediction_outlier = lof_outlier.fit_predict(X)

count=0
for l in prediction_outlier:
    if l==-1:
        count=count+1
outlier_identified=count
normals_identifed=total_sample-count
print(f"outliers Identified by LOF ={count} \n Normals identified={normals_identifed}")  
prediction_outlier = [1 if i==-1 else 0 for i in prediction_outlier]
print(classification_report(Y, prediction_outlier))   

