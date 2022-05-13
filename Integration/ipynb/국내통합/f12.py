from pymongo import MongoClient
import numpy as np
import time
#import multicpu

client = MongoClient('mongodb://203.255.92.141:27017', authSource='admin')
filter_info = client['PUBLIC']['FilterInfo'] #필터접근
filters_category = client['PUBLIC']['FilterCategory']

f_id = 0 #input
keyid = 764 #keyid

fid_key_query = filter_info.find_one({ '$and': [{ 'fId': f_id }, { 'keyId': keyid }]}) #f_id serach
ninst = []
nrsc = []
nfund = []
nyear = []
pinst = []
pyear = []
pjournal = []
plang = []

if  fid_key_query != None: #f_id check
    for key in fid_key_query.keys() :
        if key == 'nFilter':
            ninst = fid_key_query[key]['inst'] #소속
            nrsc = fid_key_query[key]['rsc'] #공동저자수
            nfund = fid_key_query[key]['fund'] #과제수주비
            nyear = fid_key_query[key]['year'] #연도

        elif key == 'pFilter' :
            pinst = fid_key_query[key]['inst']
            pyear = fid_key_query[key]['year']
            pjournal = fid_key_query[key]['journal']
            plang = fid_key_query[key]['lang']

dbpia_aut = client['DBPIA']['Author']

scion_raw = client['SCIENCEON']['Rawdata']
ntis_raw = client['NTIS']['Rawdata']
kci_raw = client['KCI']['Rawdata']
dbpia_raw = client['DBPIA']['Rawdata']

scion_key_query = scion_raw.find({ 'keyId' : keyid })
ntis_key_query = ntis_raw.find({ 'keyId' : keyid })
kci_key_query = kci_raw.find({ 'keyId' : keyid })
dbpia_key_query = dbpia_raw.find({ 'keyId' : keyid })

key_querys = [scion_key_query, ntis_key_query, kci_key_query, dbpia_key_query] #Rawdata
id_domestic = client['ID']['Domestic'] #Domestic

mng_id = [] # Author id
paper = []

Answer_dict = {} # Answer result
fp_dict = {} #filter papaer result
site = ['SCIENCEON', 'NTIS', 'KCI', 'DBPIA']
fund = [0, 50000000, 100000000, 300000000, 500000000, 1000000000, 10000000000000 ]
rsc = [0, 10, 30, 50, 100, 100000]

dbpia_mng_ids = {}
dbpia_paper_year = []
dbpia_paper_journal = []
dbpia_ori_inst = []
dbpia_paper_lang = []
dbpia_exi_inst = []
dbpia_mng_name = []
dbpia_paper = []

savetime1 = 0
savetime2 = 0
end1 = 0

f_nyear = {}
f_ninst = {}
f_nfund = {'0':0, '1':0, '2':0, '3':0, '4':0, '5':0}
f_nrsc = {'0':0, '1':0, '2':0, '3':0, '4':0}
f_pyear = {}
f_pinst = {}
f_pjournal = {}
f_plang = {}

Inte_name = []

def simple_filter(value, filters) :
    if value in filters or filters == []:
        return True
    return False
        
def complex_filter(value, filters, base) :
    if filters == [] :
        return True

    for j in range(len(filters)):
        if base[filters[j]] <= float(value) < base[filters[j]+1]:
            return True
    return False

def fc_simple_filter(category, fc_dict):
    if category not in fc_dict:
        fc_dict[category] = 0
    fc_dict[category] += 1
    return fc_dict

def fc_complex_filter(category, base, fc_dict):
    for j in range(len(base)-1):
        if base[j] <= float(category) < base[j+1]:
            fc_dict[str(j)] += 1
            return fc_dict

for i in range(len(key_querys)):
    mng_dict = {}
    start1 = time.time()
    for key_query in key_querys[i]: #rawdata(magid, paper) insert
        if site[i] == 'NTIS' :
            ori_inst = key_query['originalName']
            ntis_rsc = int(key_query['cntRscMan']) + int(key_query['cntRscWom'])
            ntis_fund = key_query['totalFund']
            ntis_year = key_query['prdStart'][:4]
            exi_inst = key_query['ldAgency']
            mng_name =  key_query['mng']
            mng_id = key_query['mngId']
            paper = key_query['_id']

            if simple_filter(ori_inst, ninst) and simple_filter(ntis_year, nyear) and complex_filter(ntis_fund, nfund, fund) and complex_filter(ntis_rsc, nrsc, rsc):
                if mng_id not in mng_dict:
                    mng_dict[mng_id] = {'name' : mng_name, 'inst' : exi_inst, 'papers' : [], 'oriInst' : ori_inst}
                mng_dict[mng_id]['papers'].append(paper)
                f_nyear = fc_simple_filter(ntis_year, f_nyear)
                f_ninst = fc_simple_filter(ori_inst, f_ninst)
                f_nfund = fc_complex_filter(ntis_fund, fund, f_nfund)
                f_nrsc = fc_complex_filter(ntis_rsc, rsc, f_nrsc)
                
        elif site[i] == 'DBPIA':
            mng_id = key_query['mngId']
            if mng_id not in dbpia_mng_ids:
                dbpia_paper_year = key_query['issue_year'][:4]
                dbpia_paper_journal = key_query['journal']
                dbpia_paper_lang = key_query['issue_lang']
                dbpia_mng_name = key_query['author'].split(';')[-2]
                dbpia_paper = key_query['_id']
                dbpia_mng_ids[mng_id] = {'year': dbpia_paper_year, 'journal': dbpia_paper_journal, 'lang': dbpia_paper_lang, 'name': dbpia_mng_name, 'paper' : dbpia_paper}
                
        else:
            if key_query['author_inst'] not in "":
                paper_year =  key_query['issue_year'][:4]
                paper_journal = key_query['journal']
                ori_inst = key_query['originalName'].split(';')[-2]
                paper_lang = key_query['issue_lang']
                exi_inst = key_query['author_inst'].split(';')[-2]
                mng_name = key_query['author'].split(';')[-2]
                mng_id = key_query['mngId']
                paper = key_query['_id']

            if simple_filter(paper_year, pyear) and simple_filter(paper_journal, pjournal) and simple_filter(ori_inst, pinst) and simple_filter(paper_lang, plang):
                if mng_id not in mng_dict:
                    mng_dict[mng_id] = {'name' : mng_name, 'inst' : exi_inst, 'papers' : [], 'oriInst' : ori_inst}
                mng_dict[mng_id]['papers'].append(paper)
                fp_dict[paper] = {'year' : paper_year, 'inst' : ori_inst, 'journal' : paper_journal, 'lang' : paper_lang}
                
    if site[i] == 'DBPIA':
        dbpia_aut_query = dbpia_aut.find({ '_id' : {'$in' : list(dbpia_mng_ids.keys())}})
        for aut_query_one in dbpia_aut_query:
            hasInst = aut_query_one['hasInst']
            if hasInst == False:
                continue
            paper_year =  dbpia_mng_ids[aut_query_one['_id']]['year']
            paper_journal = dbpia_mng_ids[aut_query_one['_id']]['journal']
            exi_inst = aut_query_one['inst']
            ori_inst = aut_query_one['originalName']
            paper_lang = dbpia_mng_ids[aut_query_one['_id']]['lang']
            mng_name = dbpia_mng_ids[aut_query_one['_id']]['name']
            mng_id = aut_query_one['_id']
            paper = dbpia_mng_ids[aut_query_one['_id']]['paper']
            
            if simple_filter(paper_year, pyear) and simple_filter(paper_journal, pjournal) and simple_filter(ori_inst, pinst) and simple_filter(paper_lang, plang):
                if mng_id not in mng_dict:
                    mng_dict[mng_id] = {'name' : mng_name, 'inst' : exi_inst, 'papers' : [], 'oriInst' : ori_inst}
                mng_dict[mng_id]['papers'].append(paper)
                fp_dict[paper] = {'year' : paper_year, 'inst' : ori_inst, 'journal' : paper_journal, 'lang' : paper_lang}
                
    end2 = time.time()
    db_time = end2-start1
    print(f'DB 수집: {site[i]}, {db_time}')
    savetime1 += db_time
    
    for mng_one in mng_dict :
        oriinst = mng_dict[mng_one]['oriInst']
        exiinst = mng_dict[mng_one]['inst']
        mng_name = mng_dict[mng_one]['name']
        paper = mng_dict[mng_one]['papers']

        Answer = {'fid': f_id, 'keyId': keyid, 'name' : mng_name , 'inst': oriinst, site[i] : {'inst' :exiinst, 'A_id': [mng_one], 'papers' : paper, 'oriInst' : oriinst} }
        
        if mng_name not in Answer_dict and mng_name+'_0' not in Answer_dict : #동명이인이 없을 때
            Answer_dict[mng_name] = Answer
        else :
            count = 0
            flag = True
            while flag :
                temp = None
                tempName = mng_name
                
                if tempName in Answer_dict : # 이름 으로만 key가ㅣ 존재         
                    temp = Answer_dict[tempName]
                    flag = False
                else :
                    tempName = mng_name+'_'+str(count) # 이름 + 숫자로 key가ㅣ 존재
                    if tempName not in Answer_dict :
                        flag = False 
                        break
                    temp = Answer_dict[tempName]
                        
                for key in temp.keys() : # 사이트 돌면서
                    if key != 'name' and key != 'keyId' and key != 'fid' and key != 'inst': 
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
                                Answer_dict[tempName][site[i]]['A_id'].extend([mng_one])
                                Answer_dict[tempName][site[i]]['papers'].extend(paper)
                                flag = False
                                break

                            elif mng_name+'_'+str(count+1) not in Answer_dict : #소속이 다를 때
                                Answer_dict[mng_name+'_'+str(count+1)] = Answer
                                if tempName == mng_name:
                                    Answer_dict[mng_name+'_0'] = temp
                                    del Answer_dict[mng_name]
                                flag = False
                                break
                            
                        else :# 사이트가 다를때 
                            if temp[key]['inst'] == exiinst  or (src != "" and src in tgt):  # 소속 같을때
                                Answer_dict[tempName][site[i]] =  {'inst' : exiinst, 'A_id': [mng_one], 'papers' : paper, 'oriInst' : oriinst}
                                Inte_name.append(tempName)
                                if '대학교' in Answer_dict[tempName][site[i]]['oriInst'] and '대학교' not in Answer_dict[tempName]['inst']:
                                    Answer_dict[tempName]['inst'] = Answer_dict[tempName][site[i]]['oriInst']
                                flag = False
                                break
                            
                            elif mng_name+'_'+str(count+1) not in Answer_dict : #소속이 다를 때
                                Answer_dict[mng_name+'_'+str(count+1)] = Answer
                                if tempName == mng_name:
                                    Answer_dict[mng_name+'_0'] = temp
                                    del Answer_dict[mng_name]
                                flag = False
                                break

                count += 1
end3 = time.time()
savetime2 = end3-end2+savetime1

print(f'2차 통합: {savetime2}')
print(sorted(Answer_dict.items()))
# print(filter_dict)
# print(Inte_name)