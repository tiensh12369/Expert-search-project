from pymongo import MongoClient
import itertools
import numpy as np
import jaro
import time

client = MongoClient('mongodb://203.255.92.141:27017', authSource='admin')
filter_info = client['PUBLIC']['FilterInfo'] #필터접근
filters_category = client['PUBLIC']['FilterCategory']

f_id = int(sys.argv[1]) #filter_id
keyid = int(sys.argv[2])  #keyid

fid_key_query = filter_info.find_one({ '$and': [{ 'fId': f_id }, { 'keyId': keyid }]}) #f_id serach

pinst = []
pyear = []
pjournal = []
plang = []

if  fid_key_query != None: #f_id check
    for key in fid_key_query.keys() :
        if key == 'pFilter':
            pinst = fid_key_query[key]['inst']
            pyear = fid_key_query[key]['year']
            pjournal = fid_key_query[key]['journal']
            plang = fid_key_query[key]['lang']

wos_raw = client['WOS']['Rawdata']
scopus_raw = client['SCOPUS']['Rawdata']

wos_key_query = wos_raw.find({ 'keyId' : keyid })
scopus_key_query = scopus_raw.find({ 'keyId' : keyid })

key_querys = [wos_key_query, scopus_key_query] #Rawdata
id_domestic = client['ID']['Domestic'] #Domestic

mng_id = [] # Author id
paper = []

Answer_dict = {} # Answer result
fp_dict = {} #filter papaer result
site = ['WOS', 'SCOPUS']

savetime1 = 0
savetime2 = 0
end1 = 0

f_pyear = {}
f_pinst = {}
f_pjournal = {}
f_plang = {}

Inte_name = []

def different_lang(name):
    # Conversion_list = ['í', 'é', 'ô', 'ń', 'ł', 'á', 'ú', 'ä']
    Conversion_list = ['i', 'e', 'o', 'n', 'l', 'a', 'u', 'a']
    Conversion_index = [237, 233, 244, 324, 322, 225, 250, 228]
    list_name = list(name)
    
    for i in range(len(list_name)):        
        if ord(list_name[i]) in Conversion_index:
            list_name[i] = Conversion_list[Conversion_index.index(ord(list_name[i]))]
            
    name = "".join(list_name)
    return name

def simple_filter(value, filters):
    if value in filters or filters == []:
        return True
    return False
        
def fc_simple_filter(category, fc_dict):
    if category not in fc_dict:
        fc_dict[category] = 0
    fc_dict[category] += 1
    return fc_dict

for i in range(len(key_querys)):
    mng_dict = {}
    start1 = time.time()
    for key_query in key_querys[i]: #rawdata(magid, paper) insert

        if key_query['author_inst'] not in "":
            paper_year =  str(key_query['issue_year'])
            paper_journal = key_query['journal']
            # ori_inst = key_query['originalName'].split(';')[-2]
            ori_inst = key_query['author_inst'].split(';')[-2]
            paper_lang = key_query['issue_lang']
            exi_inst = key_query['author_inst'].split(';')[-2]
            mng_name = key_query['author'].split(';')[-2]
            mng_id = key_query['author_id'].split(';')[-1]
            paper = key_query['_id']
            
        if simple_filter(paper_year, pyear) and simple_filter(paper_journal, pjournal) and simple_filter(ori_inst, pinst) and simple_filter(paper_lang, plang):
            if mng_id not in mng_dict:
                mng_dict[mng_id] = {'name' : mng_name, 'inst' : exi_inst, 'papers' : [], 'oriInst' : ori_inst}
            mng_dict[mng_id]['papers'].append(paper)
            fp_dict[paper] = {'year' : paper_year, 'inst' : ori_inst, 'journal' : paper_journal, 'lang' : paper_lang}
            
    end2 = time.time()
    db_time = end2-start1
    print(f'DB 수집: {site[i]}, {db_time}')
    savetime1 += db_time
    len_mng_dict = len(mng_dict)
    print(f'전체 저자 수: {len_mng_dict}')
    
    for mng_one in mng_dict :
        oriinst = mng_dict[mng_one]['oriInst']
        exiinst = mng_dict[mng_one]['inst']
        mng_name = mng_dict[mng_one]['name']
        paper = mng_dict[mng_one]['papers']
        pre_name = different_lang(mng_name.lower().replace(" ", "").replace(",", "").replace(".", "").replace("-", "").replace("´", ""))
        Answer = {'fid': f_id, 'keyId': keyid, 'name' : mng_name, 'pre_name' : pre_name, 'inst': oriinst, site[i] : {'inst' :exiinst, 'A_id': [mng_one], 'papers' : paper, 'oriInst' : oriinst} }
        
        if pre_name == 'herrerafrancisco' and site == 'SCOPUS':
            print(Answer)
        
        if pre_name not in Answer_dict and pre_name+'_0' not in Answer_dict : #동명이인이 없을 때
            Answer_dict[pre_name] = Answer

        else:
            count = 0
            flag = True
            while flag :
                temp = None
                tempName = pre_name
                
                if tempName in Answer_dict : # 이름 으로만 key가ㅣ 존재         
                    temp = Answer_dict[tempName]
                    flag = False
                else :
                    tempName = pre_name+'_'+str(count) # 이름 + 숫자로 key가ㅣ 존재
                    if tempName not in Answer_dict :
                        flag = False 
                        break
                    temp = Answer_dict[tempName]
                        
                for key in temp.keys() : # 사이트 돌면서
                    if key != 'name' and key != 'keyId' and key != 'fid' and key != 'inst' and key != 'pre_name': 
                        src = ""
                        tgt = ""

                        if len(exiinst) >= len(temp[key]['inst']):
                            src = temp[key]['inst']
                            tgt = exiinst

                        elif len(exiinst) < len(temp[key]['inst']):
                            src = exiinst
                            tgt = temp[key]['inst']

                        if key == site[i] :# 사이트가 동일할때
                            if temp[key]['inst'] == exiinst or (src != "" and src in tgt) :  # 소속 같을때
                                Inte_name.append(tempName)
                                Answer_dict[tempName][site[i]]['A_id'].extend([mng_one])
                                Answer_dict[tempName][site[i]]['papers'].extend(paper)
                                flag = False
                                break

                            elif pre_name+'_'+str(count+1) not in Answer_dict : #소속이 다를 때
                                Answer_dict[pre_name+'_'+str(count+1)] = Answer
                                if tempName == pre_name:
                                    Answer_dict[pre_name+'_0'] = temp
                                    del Answer_dict[pre_name]
                                flag = False
                                break
                            
                        else :# 사이트가 다를때 
                            if temp[key]['inst'] == exiinst  or (src != "" and src in tgt):  # 소속 같을때
                                Answer_dict[tempName][site[i]] =  {'inst' : exiinst, 'A_id': [mng_one], 'papers' : paper, 'oriInst' : oriinst}
                                Inte_name.append(tempName)
                                if  Answer_dict[tempName]['inst'] == "" or Answer_dict[tempName]['inst'] == " ":
                                    Answer_dict[tempName]['inst'] = Answer_dict[tempName][site[i]]['oriInst']
                                flag = False
                                break
                            
                            elif pre_name+'_'+str(count+1) not in Answer_dict : #소속이 다를 때
                                Answer_dict[pre_name+'_'+str(count+1)] = Answer
                                if tempName == pre_name:
                                    Answer_dict[pre_name+'_0'] = temp
                                    del Answer_dict[pre_name]
                                flag = False
                                break

                count += 1
end3 = time.time()
savetime2 = end3-end2+savetime1

print(f'2차 통합: {savetime2}')
print(Inte_name)
print(len(Answer_dict))

