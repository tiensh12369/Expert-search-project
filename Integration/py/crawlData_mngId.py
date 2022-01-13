from pymongo import MongoClient
import pandas as pd
import numpy as np
from bson.objectid import ObjectId

client = MongoClient('mongodb://203.255.92.141:27017', authSource='admin')
db = client['DBPIA']



def __main__():
    input_keyid = 662
    input_site = ["SCIENCEON" , "KCI", "NTIS"] 
    a = crawling_name_inst()
    a.author_crawl(input_keyid,input_site)
     
class crawling_name_inst:
    def __init__(self):
        self.client = MongoClient('mongodb://203.255.92.141:27017', authSource='admin')
        
         
    def author_crawl(self, keyid, site):
        self.file_data = open(f'answer{keyid}.csv', 'w', encoding='utf-8-sig')
        for j in site:

            mngId = []
            for i in self.client[j]['Rawdata'].find({"keyId":keyid}):
                mngId.append(i['mngId'])

            result = set(mngId)
            print(j, len(result))
           


    def author_info(self, aid, site):
        for i in self.client[site]['Author'].find({'_id':aid}):
            x = i['name'] + '' + site + '' + i['inst'] + '\n'
            self.file_data.write(x)

__main__()