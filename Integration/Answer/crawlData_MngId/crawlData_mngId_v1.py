from os import replace
from pymongo import MongoClient
import pandas as pd
import numpy as np
from bson.objectid import ObjectId

client = MongoClient('mongodb://203.255.92.141:27017', authSource='admin')
db = client['DBPIA']

def author_info(aid):
    print(aid)
    

def __main__():
    input_keyid = 990
    input_site = ["SCOPUS" , "WOS"] 
    a = crawling_name_inst()
    a.author_crawl(input_keyid, input_site)
     
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
            for i in result:
                self.author_info(i, j)
        self.file_data.close()
        print(site, "끝")


    def author_info(self, aid, site):
        for i in self.client[site]['Author'].find({'_id':aid}):
            # i.replace(',' , ';')
            x = i['name'] + ',' + site + ',' + i['inst'].replace(',',';') + '\n' 
            # x = i['name'] + site + i['inst'] + '\n' 
            self.file_data.write(x)

__main__()