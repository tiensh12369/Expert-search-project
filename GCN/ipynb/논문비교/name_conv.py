from pymongo import MongoClient
import pandas as pd
import numpy as np
from bson.objectid import ObjectId
import pprint
import itertools

f = open("C:/Users/정상준/Documents/네이트온 받은 파일/test.txt", 'w')

client = MongoClient('mongodb://203.255.92.141:27017', authSource='admin')

scion_row = client['SCIENCEON']['Rawdata']
ntis_row = client['NTIS']['Rawdata']
dbpia_row = client['DBPIA']['Rawdata']

raw_dbs = {'NTIS' : ntis_row, 'Scienceon' : scion_row, 'DBPIA' : dbpia_row}

sites = ['NTIS', 'Scienceon', 'DBPIA']

data0 = {
        '이지현' : {'fid': 0, 'keyId': 588, 'name': '이지현', 'NTIS': {'inst': '한국과학기술원', 'A_id': '809210816201', 'papers': [ObjectId('6193944674c5e6d7a8ac4c00'), ObjectId('6193944674c5e6d7a8ac4c2c'), ObjectId('6193944774c5e6d7a8ac4c5c')]}},
        '복경수_3': {'fid': 0, 'keyId': 588, 'name': '복경수', 'Scienceon': {'inst': '충북대학교', 'A_id': ['s258056'], 'papers': [ObjectId('61939443e9b04a9d64abe055')]}, 'DBPIA': {'inst': '충북대학교', 'A_id': ['253221'], 'papers': [ObjectId('6193944905488d4887ad9265'), ObjectId('6193945005488d4887ad92ec'), ObjectId('6193945605488d4887ad939a'), ObjectId('6193945c05488d4887ad9484')]}}, \
        '복경수_1': {'fid': 0, 'keyId': 588, 'name': '복경수', 'Scienceon': {'inst': '원광대학교', 'A_id': ['s261881'], 'papers': [ObjectId('61939447e9b04a9d64abe06a'), ObjectId('6193944ce9b04a9d64abe06f')]}, 'DBPIA': {'inst': '원광대학교', 'A_id': ['4366954'], 'papers': [ObjectId('6193945d05488d4887ad949d')]}}, \
        '복경수_2': {'fid': 0, 'keyId': 588, 'name': '복경수', 'DBPIA': {'inst': 'Wonkwang', 'A_id': ['3946247'], 'papers': [ObjectId('6193945c05488d4887ad9471'), ObjectId('6193945c05488d4887ad9471')]}}, \
        '이수현' : {'fid': 0, 'keyId': 588, 'name': '이지현', 'NTIS': {'inst': '한국과학기술원', 'A_id': '809210816201', 'papers': [ObjectId('6193944674c5e6d7a8ac4c00'), ObjectId('6193944674c5e6d7a8ac4c2c'), ObjectId('6193944774c5e6d7a8ac4c5c')]}}, \
        '복경수_0': {'fid': 0, 'keyId': 588, 'name': '복경수', 'DBPIA': {'inst': '', 'A_id': ['4627944'], 'papers': [ObjectId('6193945c05488d4887ad9475'), ObjectId('6193945c05488d4887ad9475')]}}, \
         }

def test_filter(name, site1, raw1, site2, raw2) :
    return 3

def getRaw(name):
    if 'raws' not in data0[name]:
        raws = []
        for site in sites:
            if site in data0[name]:
                for c in raw_dbs[site].find({"_id": {"$in": data0[name][site]['papers']}}):
                    c['site'] = site
                    raws.append(c)
        
        data0[name]['raws'] = raws

processedList = []
deleteList = []

for data in data0 :
    if '_' in data :
        name = data.split("_")
        if name[0] in processedList :
            continue
        preprocessedList = []
        c = 0
        while True :
            pname = name[0]+"_"+str(c)
            if pname in data0 :            
                preprocessedList.append(pname)
                getRaw(pname)
                c += 1
            else :
                break
        
        processedList.append(name[0])
        flag = True
        while flag : 
            flag = False
            pairs =list(itertools.combinations(preprocessedList, 2))
            #[김상혁_0, 김상혁_1][김상혁_0, 김상혁_2]
            
            result = test_filter(name[0], site1, raw1, site2, raw2)
            
            for pair in pairs:
                pair = list(pair)
                '''
                # <2차통합 유사도 함수 자리>
                # 2차통합유사도함수_상혁(data0[pair[0]]['raws'], data0[pair[1]]['raws'])
                # return 값으로 두 명에 대한 유사도 값이 나옴
                # 6 이상이면 통합 / 임시로 result 변수로 사용
                '''
                

                if result >= 6:
                    deleteList.append(pair[1])
                    for site in sites:
                        if site in data0[pair[1]]:
                            if site in data0[pair[0]].keys() :                            
                                data0[pair[0]][site]['A_id'].extend(data0[pair[1]][site]['A_id'])
                                data0[pair[0]][site]['papers'].extend(data0[pair[1]][site]['papers'])
                                data0[pair[0]]['raws'].extend(data0[pair[1]]['raws'])
                            else:
                                data0[pair[0]][site] = data0[pair[1]][site]
                    flag = True
                    
                    preprocessedList.remove(pair[1])
                    break
        
for d in deleteList:
    del data0[d]
print(data0.keys())