class Spray:
    def __init__(self, total_sample,node_num, X): 
        self.node_num=node_num
        self.total_sample=total_sample
        self.X=X
        
    
    def distribute(self):
        node_count=0
        nodes=[ [] for _ in range(self.node_num) ]
        tot=self.total_sample
        if(self.total_sample % self.node_num==0):
            div=self.total_sample/self.node_num
        elif(self.total_sample % self.node_num!=0 and self.node_num!=1):
            div=self.total_sample/(self.node_num-1)
            tot=int(div)*(self.node_num-1)
        else:
            div=self.total_sample
        for i in range(0,tot,int(div)):
            for j in range(i,i+int(div)):
                nodes[node_count].append(self.X[j])
            node_count=node_count+1
    
        if(self.total_sample % self.node_num!=0 and self.node_num!=1):
            for m in range(tot, self.total_sample):
                nodes[self.node_num-1].append(self.X[m])
        
        return nodes



    
