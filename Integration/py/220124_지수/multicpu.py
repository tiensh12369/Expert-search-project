
from multiprocessing import Process, Queue
import multiprocessing
import os
from bson.objectid import ObjectId
from pymongo import MongoClient
from new_analyzer import run
import sys

# def __main__(keyid, f_id):
#     keyid = sys.argv[0]
#     f_id = sys.argv[1]
#     analyzer = run_factor_integration(keyid, f_id)
    
#     analyzer.run()
#    # analyzer.factor_norm()

class run_factor_integration:
    def __init__(self, keyid, fid):
        self.client =  MongoClient('203.255.92.141:27017', connect=False)
        self.PUBLIC = self.client['PUBLIC']
        self.new_max_factor = self.PUBLIC['new_factor'] 
        self.ID = self.client['ID']
        self.Domestic = self.ID['Domestic']
        self.keyid = keyid
        self.fid = fid
        
        self.DATA = self.ID['Domestic'].find({"keyId":self.keyid, "fid":fid})


    def count_people(self):
        cnt = 0
      #  print(cnt)
       # print("실행")
        for i in self.DATA:
            #print(i)
            cnt += 1
        return cnt
    

    def run(self):
     
     #   print("count_people", self.count_people)
        cnt = self.count_people()
        processList = []
        if None == self.new_max_factor.find_one({'keyId': self.keyid}):
            self.new_max_factor.insert({'keyId': self.keyid},{'keyId': self.keyid, 'Quality' : -1, 'accuracy' : -1, 'recentness' : -1, 'coop': -1 })

        # if __name__ == '__main__':

        for i in range(0,cnt , 100):
            start = 1 *i
            end = 100
            if i//100 == cnt//100:
                start = i
                end = cnt
            print(end)
            
            proc = Process(target=run(start, end, self.fid, self.keyid),daemon = False)
            
            processList.append(proc)
            proc.start()


        for p in processList :
            p.join()
        
        self.factor_norm()
        
        


    def factor_norm(self):
        max_factor = self.new_max_factor.find({'keyId':self.keyid})
        for doc1 in max_factor:
            max_qual = doc1['Quality']
            max_acc = doc1['accuracy']
            max_recentness = doc1['recentness']
            max_coop = doc1['coop']
            update_list = self.Domestic.find({"keyId":self.keyid, 'fid': self.fid})
            for doc in update_list:
                # print(doc)
                if max_qual != 0:
                    norm_qual = doc['factor']['qual']/max_qual
                else:
                    norm_qual = doc['factor']['qual']
                score = norm_qual * 25 + doc['factor']['acc'] *25 + doc['factor']['recentness'] * 25 +  doc['factor']['coop'] * 25
                self.Domestic.update({'_id':ObjectId(doc['_id'])},{"$set":{'score':score ,'factor':{"qual":norm_qual,'coop':doc['factor']['coop'],'recentness':doc['factor']['recentness'],'acc':doc['factor']['acc']}}})
        print('end')
            
