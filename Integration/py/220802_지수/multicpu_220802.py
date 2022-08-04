from multiprocessing import Process
from pymongo import MongoClient
import multiprocessing
from time import sleep
from nltk.corpus import stopwords
from new_analyzer_3 import factor_integration


def __main__():
    f_id = 0 #input
    keyid = 1110
    analyzer = run_factor_integration(keyid, f_id)
    analyzer.run()
    analyzer.factor_norm()


class run_factor_integration:
    def __init__(self, keyid, fid):
        self.cores = multiprocessing.cpu_count()
        if self.cores > 3 :
            self.cores -= 1
        self.client =  MongoClient('203.255.92.141:27017', connect=False)
        self.PUBLIC = self.client['PUBLIC']
        self.new_max_factor = self.PUBLIC['new_factor']
        self.ID = self.client['ID']
        self.Domestic = self.ID['Domestic']
        self.keyid = keyid
        self.fid = fid

        # stopwards
        en_stop_words = stopwords.words("english")
        ko_stop_words = []
        with open('/home/search/apps/product/consumer/ko_stopword.txt', 'r', encoding='utf-8') as f:
            lines = f.readlines()
            for line in lines:
                x = line.strip()
                ko_stop_words.append(x)

        self.stop_words = en_stop_words + ko_stop_words


    def run(self):
        print("multicpu run")
        authorSize = self.ID['Domestic'].find({"keyId":self.keyid, "fid":self.fid}).count()
        processList = []
        self.new_max_factor.remove({"keyId":self.keyid})
        if None == self.new_max_factor.find_one({'keyId': self.keyid,"fid":self.fid}):
            print("factor initialization")
            self.new_max_factor.insert({'keyId': self.keyid,"fid":self.fid,'keyId': self.keyid,"fid":self.fid,
                                        'ntisQual' : -1, 'remainQual' : -1,'accuracy' : -1, 'recentness' : -1, 'coop': -1 })

        th = 100 # each core handle 100 or more data
        sizeDict = {}
        perData = int(authorSize / self.cores)
        if perData > th :
            last = 0
            for i in range(self.cores-1) :
                sizeDict[last] = last+perData
                last += perData
            sizeDict[last] = authorSize
        else :
            sizeDict[0] = authorSize

        processList = []
        for key in sizeDict :
            acl = None
            acl = factor_integration(key, sizeDict[key], self.fid ,self.keyid, self.stop_words)
            print("analyzer run")

            p = Process(target= acl.run)
            processList.append(p)
            p.start()

        for p in processList :
            p.join()

        self.factor_norm()

    def factor_norm(self):
        max_factor = self.new_max_factor.find_one({'keyId':self.keyid})

        if max_factor['ntisQual'] != None :
            max_ntisQual = max_factor['ntisQual']
        else:
            max_ntisQual = 0

        max_remainQual = max_factor['remainQual']

        if max_ntisQual != 0:
            norm_ntisQual_factor = 1 / max_ntisQual
        else:
            norm_ntisQual_factor = 0

        norm_remainQual_factor = 1 / max_remainQual

        max_recentness = max_factor['recentness']
        max_coop = max_factor['coop']
        real_recentness = 1 / max_recentness
        print("real_recentness : ",real_recentness,"max_coop: ",max_coop,"norm_remainQual_factor : ",norm_remainQual_factor, "norm_ntisQual_factor",norm_ntisQual_factor)

        if max_coop == 0: real_coop = max_coop
        elif max_coop != 0: real_coop = 1/ max_coop

        self.ID['test'].update_many(
            {"keyId":self.keyid, "fid":self.fid}, [
                {"$set" : {
                    "factor.coop":{"$multiply": ["$factor.coop", real_coop]},
                    "factor.qunt":{
                        "$sum":[
                        {"$multiply": [real_recentness, "$factor.qunt", 0.66]},
                        "$factor.lct"
                        ]},
                    "factor.qual":{
                            "$sum":[
                                {"$multiply" : ["$factor.ntisQual", norm_ntisQual_factor,0.5]},
                                {"$multiply" : ["$factor.remainQual", norm_remainQual_factor,0.5]}
                            ]},
                    "score" : {
                        '$sum':[
                        {"$multiply": [
                            {"$sum" : [
                                {"$multiply" : ["$factor.ntisQual", norm_ntisQual_factor,0.5]},
                                {"$multiply" : ["$factor.remainQual", norm_remainQual_factor,0.5]}
                            ]}, 30]},
                        {"$multiply": ["$factor.acc", 30]},
                        {"$multiply": ["$factor.coop", 10]},
                        {"$multiply": ["$factor.qunt",30]}
                            ]}}}])

        print("종료")

__main__()
