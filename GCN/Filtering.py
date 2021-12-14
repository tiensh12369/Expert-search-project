import sys
from pymongo import MongoClient

client = MongoClient('mongodb://203.255.92.141:27017', authSource='admin')
filter_info = client['PUBLIC']['FilterInfo'] #필터접근

f_id = int(sys.argv[1]) #input
keyid = int(sys.argv[2])  #keyid

nyear = []
pyear = []

fid_key_query = filter_info.find_one({'fId' : f_id }) #필터검색

if  f_id > 0 and fid_key_query != None: #필터쿼리가 있으면
    for key in fid_key_query.keys() :
        if key ==  'nFilter':
            nyear = fid_key_query[key]['year']
        elif key == 'pFilter' :
            pyear = fid_key_query[key]['year']

scion_aut = client['SCIENCEON']['Author']
ntis_aut = client['NTIS']['Author']
dbpia_aut = client['DBPIA']['Author']

scion_autpaper = client['SCIENCEON']['AuthorPapers']
ntis_autpaper = client['NTIS']['AuthorPapers']
dbpia_autpaper = client['DBPIA']['AuthorPapers']

scion_raw = client['SCIENCEON']['Rawdata']
ntis_raw = client['NTIS']['Rawdata']
dbpia_raw = client['DBPIA']['Rawdata']

scion_key_query = scion_autpaper.find({ 'keyId' : keyid })
ntis_key_query = ntis_autpaper.find({ 'keyId' : keyid })
dbpia_key_query = dbpia_autpaper.find({ 'keyId' : keyid })

auts = [scion_aut, ntis_aut, dbpia_aut] #저자 이름, 소속
key_querys = [scion_key_query, ntis_key_query, dbpia_key_query] #a_id

a_id = []
all_name = []
all_inst = []
all_site = []
Answer_dict = {} #통합결과

site = ['Scienceon', 'NTIS', 'DBPIA']

for i in range(len(key_querys)):
    for key_query in key_querys[i]: #keyid에 저자수만큼 반복
        Aid = []
        A_id = []
        all_paper = []
        raw_data = {}

        a_id.append(key_query['A_ID']) #a_id read

        if site[i] == 'NTIS' : 
            ntis_raw_query = ntis_raw.find({'$and':[{'keyId':keyid},{'mngId':a_id[-1]}]})
            
            if ntis_raw_query == None:
                continue
            else:
                for raw_one in ntis_raw_query:
                    if f_id < 1 and fid_key_query == None:
                        Aid = a_id[-1]
                        all_paper = key_query['papers']

                    elif int(raw_one['prdEnd'][:4]) in nyear or int(raw_one['prdStart'][:4]) in nyear : #필터링
                        Aid = a_id[-1]
                        all_paper = key_query['papers']

        elif site[i] == 'Scienceon' :
            scion_raw_query = scion_raw.find({'$and':[{'keyId':keyid},{'author_id':{'$regex':a_id[-1]}}]})
            if scion_raw_query == None :
                continue
            else:
                for raw_one in scion_raw_query:
                    if f_id < 1 and fid_key_query == None:
                        A_id.append(a_id[-1])
                        for v in A_id:
                            if v not in Aid:
                                Aid.append(v)
                                all_paper = key_query['papers']

                    elif int(raw_one['issue_year'][:4]) in pyear: #필터링
                        A_id.append(a_id[-1])
                        for v in A_id:
                            if v not in Aid:
                                Aid.append(v)
                                all_paper = key_query['papers']

        elif site[i] == 'DBPIA' :
            dbpia_raw_query = dbpia_raw.find({'$and':[{'keyId':keyid},{'author_id':{'$regex':a_id[-1]}}]})
            if dbpia_raw_query == None :
                continue
            else:
                for raw_one in dbpia_raw_query:
                    if f_id < 1 and fid_key_query == None:
                        A_id.append(key_query['A_ID'])
                        for v in A_id:
                            if v not in Aid:
                                Aid.append(v)
                                all_paper = key_query['papers']

                    elif int(raw_one['issue_year'][:4]) in pyear: #필터링
                        A_id.append(key_query['A_ID'])
                        for v in A_id:
                            if v not in Aid:
                                Aid.append(v)
                                all_paper = key_query['papers']
                                
        if Aid == [] or Aid == "":
            continue

        aut_query = auts[i].find_one({'_id':key_query['A_ID']}) #저자이름, 소속 read

        all_name.append(aut_query['name'])
        all_inst.append(aut_query['inst'].replace("(주) ", "").replace("(주)", "").split(' ')[0])
        all_site.append(site[i])

        Answer = {'fid': f_id, 'keyId': keyid, 'name' : all_name[-1], site[i] : {'inst' : all_inst[-1], 'A_id': Aid, 'papers' : all_paper} }

        if all_name[-1] not in Answer_dict and all_name[-1]+'0' not in Answer_dict : #동명이인이 없을 때
            Answer_dict[all_name[-1]] = Answer
        else :
            
            count = 0
            flag = True
            while flag :
                temp = None 
                tempName = all_name[-1]
              
                if tempName in Answer_dict : # 이름 으로만 key가ㅣ 존재         
                    temp = Answer_dict[tempName]
                    flag = False
                else :
                    tempName = all_name[-1]+'_'+str(count) # 이름 + 숫자로 key가ㅣ 존재
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
                                Answer_dict[tempName][site[i]]['papers'].extend(all_paper)
                                flag = False
                                break

                            elif all_name[-1]+'_'+str(count+1) not in Answer_dict : #소속이 다를 때
                                Answer_dict[all_name[-1]+'_'+str(count+1)] = Answer

                                if tempName == all_name[-1]:
                                    Answer_dict[all_name[-1]+'_0'] = temp
                                    del Answer_dict[all_name[-1]]

                        else :# 사이트가 다를때 
                            if temp[key]['inst'] == all_inst[-1] or (src != "" and src in tgt):  # 소속 같을때
                                    Answer_dict[tempName][site[i]] =  {'inst' : all_inst[-1], 'A_id': Aid, 'papers' : all_paper}
                                    flag = False
                                    break
                            
                            elif all_name[-1]+'_'+str(count+1) not in Answer_dict : #소속이 다를 때
                                Answer_dict[all_name[-1]+'_'+str(count+1)] = Answer

                                if tempName == all_name[-1]:
                                    Answer_dict[all_name[-1]+'_0'] = temp
                                    del Answer_dict[all_name[-1]]

                count += 1

print(sorted(Answer_dict.items()))

id_domestic = client['ID']['Domestic']
#id_domestic.insert_many(Answer_dict.values())