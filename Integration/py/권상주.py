from pymongo import MongoClient
import numpy as np


client = MongoClient('mongodb://203.255.92.141:27017', authSource='admin')
filter_info = client['PUBLIC']['FilterInfo'] #필터접근

f_id = 0 #input
keyid = 676 #keyid

fid_key_query = filter_info.find_one({'fId' : f_id }) #f_id serach

if  f_id > 0 and fid_key_query != None: #f_id check
    for key in fid_key_query.keys() :
        if key ==  'nFilter':
            nyear = fid_key_query[key]['year']
        elif key == 'pFilter' :
            pyear = fid_key_query[key]['year']

scion_aut = client['SCIENCEON']['Author']
ntis_aut = client['NTIS']['Author']
kci_aut = client['KCI']['Author']

scion_raw = client['SCIENCEON']['Rawdata']
ntis_raw = client['NTIS']['Rawdata']
kci_raw = client['KCI']['Rawdata']

scion_key_query = scion_raw.find({ 'keyId' : keyid })
ntis_key_query = ntis_raw.find({ 'keyId' : keyid })
kci_key_query = kci_raw.find({ 'keyId' : keyid })

auts = [scion_aut, ntis_aut, kci_aut] #Author
key_querys = [scion_key_query, ntis_key_query, kci_key_query] #Rawdata
id_domestic = client['ID']['Domestic'] #Domestic

A_id = [] # Author id
all_name = [] # Author name
all_inst = [] # Author inst
Answer_dict = {} # Answer result
site = ['SCIENCEON', 'NTIS', 'KCI']

for i in range(len(key_querys)):
    mngid_dict = {} #mngid, paper value dict / site마다

    for key_query in key_querys[i]: #rawdata(magid, paper) insert

        if f_id < 1 and fid_key_query == None:
            if key_query['mngId'] not in mngid_dict:
                mngid_dict[key_query['mngId']] = []
            mngid_dict[key_query['mngId']].append(key_query['_id'])

        elif site[i] == 'NTIS' : 
            if key_query['prdEnd'][:4] in nyear or key_query['prdStart'][:4] in nyear : #필터링
                if key_query['mngId'] not in mngid_dict:
                    mngid_dict[key_query['mngId']] = []
                mngid_dict[key_query['mngId']].append(key_query['_id'])

        else:
            if (key_query['issue_year'][:4]) in pyear: #필터링
                if key_query['mngId'] not in mngid_dict:
                    mngid_dict[key_query['mngId']] = []
                mngid_dict[key_query['mngId']].append(key_query['_id'])

    paper = []
    aut_querys = auts[i].find({'_id': { '$in' : list(mngid_dict.keys())}})
    for aut_query in aut_querys :
        A_id = aut_query['_id']
        paper = mngid_dict[A_id]
        all_name.append(aut_query['name'])
        all_inst.append(aut_query['inst'].replace("(주) ", "").replace("(주)", "").split(' ')[0])
        all_inst.append(aut_query['inst'])
        A_id = [A_id]
        Answer = {'fid': f_id, 'keyId': keyid, 'name' : all_name[-1], site[i] : {'inst' : all_inst[-1], 'A_id': A_id, 'papers' : paper} }

        if all_name[-1] not in Answer_dict and all_name[-1]+'_0' not in Answer_dict : #동명이인이 없을 때
            Answer_dict[all_name[-1]] = Answer
        else :
            count = 0
            flag = True
            while flag :
                temp = None 
                tempName = all_name[-1]
                print(f'위: {tempName}')
                if tempName == '권상주':
                    print(Answer_dict[tempName], tempName)
                    #print(Answer_dict[tempName+'_0'])
                if tempName in Answer_dict : # 이름 으로만 key가ㅣ 존재         
                    temp = Answer_dict[tempName]
                    flag = False
                else :
                    
                    tempName = all_name[-1]+'_'+str(count) # 이름 + 숫자로 key가ㅣ 존재
                    print(tempName)                    
                    if tempName not in Answer_dict :
                        flag = False 
                        break
                    temp = Answer_dict[tempName]
                      
                for key in temp.keys() : # 사이트 돌면서
                    if key != 'name' and key != 'keyId' and key != 'fid' : 
                        src = ""
                        tgt = ""

                        if len(all_inst[-1]) >= len(temp[key]['inst']):
                            src = temp[key]['inst']
                            tgt = all_inst[-1]

                        elif len(all_inst[-1]) < len(temp[key]['inst']):
                            src = all_inst[-1]
                            tgt = temp[key]['inst']

                        if key == site[i] :# 사이트가 동일할때
                            if temp[key]['inst'] == all_inst[-1] or (src != "" and src in tgt) :  # 소속 같을때
                                if tempName == '권상주':
                                    print(Answer_dict)
                                Answer_dict[tempName][site[i]]['A_id'].extend(A_id)
                                Answer_dict[tempName][site[i]]['papers'].extend(paper)
                                flag = False
                                break

                            elif all_name[-1]+'_'+str(count+1) not in Answer_dict : #소속이 다를 때
                                Answer_dict[all_name[-1]+'_'+str(count+1)] = Answer

                                if tempName == all_name[-1]:
                                    Answer_dict[all_name[-1]+'_0'] = temp
                                    del Answer_dict[all_name[-1]]
                                flag = False
                                break

                        else :# 사이트가 다를때 
                            if temp[key]['inst'] == all_inst[-1] or (src != "" and src in tgt):  # 소속 같을때
                                    Answer_dict[tempName][site[i]] =  {'inst' : all_inst[-1], 'A_id': A_id, 'papers' : paper}
                                    flag = False
                                    break
                            
                            elif all_name[-1]+'_'+str(count+1) not in Answer_dict : #소속이 다를 때
                                Answer_dict[all_name[-1]+'_'+str(count+1)] = Answer

                                if tempName == all_name[-1]:
                                    Answer_dict[all_name[-1]+'_0'] = temp
                                    del Answer_dict[all_name[-1]]
                                flag = False
                                break

                count += 1