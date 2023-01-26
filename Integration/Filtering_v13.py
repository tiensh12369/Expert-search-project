from pymongo import MongoClient
import itertools
import numpy as np
import jaro
import time
import sys
import os
import multicpu_220504

client = MongoClient('mongodb://203.255.92.141:27017', authSource='admin')
filter_info = client['PUBLIC']['FilterInfo'] #필터접근
filters_category = client['PUBLIC']['FilterCategory']

f_id = int(sys.argv[1]) #input
keyid = int(sys.argv[2])  #keyid

fid_key_query = filter_info.find_one({ '$and': [{ 'fId': f_id }, { 'keyId': keyid }]}) #f_id serach
ninst = []
nrsc = []
nfund = []
nyear = []
pinst = []
pyear = []
pjournal = []

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

Inte_name = []

def simple_filter(value, filters) :
    if value in filters or filters == []:
        return True
    return False
        
def complex_filter(value, filters, base) :
    if filters == []:
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
            ntis_year = str(key_query['prdStart'][:4])
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
                dbpia_paper_year = str(key_query['issue_year'][:4])
                dbpia_paper_journal = key_query['journal']
                dbpia_mng_name = key_query['author'].split(';')[-2]
                dbpia_paper = key_query['_id']
                dbpia_mng_ids[mng_id] = {'year': dbpia_paper_year, 'journal': dbpia_paper_journal, 'name': dbpia_mng_name, 'paper' : dbpia_paper}
                
        else:
            if key_query['author_inst'] not in "":
                paper_year =  str(key_query['issue_year'][:4])
                paper_journal = key_query['journal']
                ori_inst = key_query['originalName'].split(';')[-2]
                exi_inst = key_query['author_inst'].split(';')[-2]
                mng_name = key_query['author'].split(';')[-2]
                mng_id = key_query['mngId']
                paper = key_query['_id']

            if simple_filter(paper_year, pyear) and simple_filter(paper_journal, pjournal) and simple_filter(ori_inst, pinst):
                if mng_id not in mng_dict:
                    mng_dict[mng_id] = {'name' : mng_name, 'inst' : exi_inst, 'papers' : [], 'oriInst' : ori_inst}
                mng_dict[mng_id]['papers'].append(paper)
                fp_dict[paper] = {'year' : paper_year, 'inst' : ori_inst, 'journal' : paper_journal, }
                
    if site[i] == 'DBPIA':
        dbpia_aut_query = dbpia_aut.find({ '_id' : {'$in' : list(dbpia_mng_ids.keys())}})
        for aut_query_one in dbpia_aut_query:
            hasInst = aut_query_one['hasInst']
            if hasInst == False:
                continue
            paper_year =  str(dbpia_mng_ids[aut_query_one['_id']]['year'])
            paper_journal = dbpia_mng_ids[aut_query_one['_id']]['journal']
            exi_inst = aut_query_one['inst']
            ori_inst = aut_query_one['originalName']
            mng_name = dbpia_mng_ids[aut_query_one['_id']]['name']
            mng_id = aut_query_one['_id']
            paper = dbpia_mng_ids[aut_query_one['_id']]['paper']
            
            if simple_filter(paper_year, pyear) and simple_filter(paper_journal, pjournal) and simple_filter(ori_inst, pinst):
                if mng_id not in mng_dict:
                    mng_dict[mng_id] = {'name' : mng_name, 'inst' : exi_inst, 'papers' : [], 'oriInst' : ori_inst}
                mng_dict[mng_id]['papers'].append(paper)
                fp_dict[paper] = {'year' : paper_year, 'inst' : ori_inst, 'journal' : paper_journal}
                
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

def filter(site, rawdata):
    if site == 'NTIS' :
        coauthor = rawdata['rsc'].split(";")
        year = int(rawdata['prdStart'][:4])
        keyword = rawdata['koKeyword'].split(",")
        journal = ""
        conference = ""
        title = rawdata['koTitle']

    else :
        coauthor = rawdata['author'].split(";")[:-1]
        year = int(rawdata['issue_year'][:4])
        paper_keyword = rawdata['paper_keyword']
        
        if paper_keyword == [] or paper_keyword is None:
            keyword = []
        elif len(paper_keyword) > 1:
            for i in range(0, len(paper_keyword)):
                keyword = []
                keyword.append(paper_keyword[i].replace(" ", "").split("."))
        else:
            keyword = paper_keyword.replace(" ", "").split(".")

        journal = rawdata['journal']
        conference = rawdata['issue_inst']
        title = rawdata['title']

    return coauthor, year, keyword, journal, conference, title

def Secondary_filter(name, site1, inst1, raw_one1, site2, inst2, raw_one2):
    inst = 0
    weight = 0
    joc = 0
    coauthor1, year1, keyword1, journal1, conference1, title1 = filter(site1, raw_one1)
    coauthor2, year2, keyword2, journal2, conference2, title2 = filter(site2, raw_one2)

    if inst1 == inst2:
        inst = 1
    else:
        inst = jaro.jaro_winkler_metric(inst1, inst2)

    if name in coauthor1:
        coauthor1.remove(name)

    if name in coauthor2:
        coauthor2.remove(name)
    
    co_author_count = len([i for i in coauthor1 if i in coauthor2])

    if site1 != 'NTIS' and site2 != 'NTIS' :
        if title1 == title2 or inst >= 0.8:
            weight = 4
            return weight

        else:
            joc = 1 if journal1 == journal2 and conference1 == conference2 else 0
    else:
        if inst >= 0.8:
            weight = 3
            return weight
            
    yop = -(2*(abs(year1-year2)/10)-1)
            
    if len(coauthor1) == 0 or len(coauthor2) == 0:
        co_author_ratio = 0
    elif len(coauthor1) < len(coauthor2):
        co_author_ratio = co_author_count/len(coauthor1)
    else:
        co_author_ratio = co_author_count/len(coauthor2)
    
    if co_author_ratio == 1:
        co_authorship = 1
    else:
        co_authorship = (1 - np.exp(-co_author_count))/2 + (co_author_ratio/2)
    keyword = 1 - np.exp(-len([i for i in keyword1 if i in keyword2]))

    weight = joc + yop + co_authorship + keyword

    return weight
    
raw_dbs = {'NTIS' : ntis_raw, 'SCIENCEON' : scion_raw, 'KCI' : kci_raw, 'DBPIA': dbpia_raw}
savetime1 = 0
savetime2 = 0
def getRaw(name):
    if 'raws' not in Answer_dict[name]:
        raws = []
        raw_one = {}
        for site_one in site:
            if site_one in Answer_dict[name]:
                for c in raw_dbs[site_one].find({"_id": {"$in": Answer_dict[name][site_one]['papers']}}):
                    raw_one['site'] = site_one
                    if site_one == 'NTIS':
                        raw_one['rsc'] = c['rsc']
                        raw_one['prdStart'] = c['prdStart']
                        raw_one['koKeyword'] = c['koKeyword']
                        raw_one['koTitle'] = c['koTitle']
                    else:
                        raw_one['author'] = c['author']
                        raw_one['issue_year'] = c['issue_year']
                        raw_one['paper_keyword'] = c['paper_keyword']
                        raw_one['journal'] = c['journal']
                        raw_one['issue_inst'] = c['issue_inst']
                        raw_one['title'] = c['title']

                    raws.append(raw_one)
        
        Answer_dict[name]['raws'] = raws

processedList = []
deleteList = []

for Answer_one in Answer_dict :

    if '_' in Answer_one :
        start1 = time.time()
        name = Answer_one.split("_")
        if name[0] in processedList :
            continue
        preprocessedList = []
        c = 0
        while True :
            pname = name[0]+"_"+str(c)
            if pname in Answer_dict :            
                preprocessedList.append(pname)
                getRaw(pname)
                c += 1
            else :
                break
        end1 = time.time()
        savetime1 += end1 - start1
        processedList.append(name[0])
        flag = True
        while flag :
            flag = False
            pairs =list(itertools.combinations(preprocessedList, 2))
            
            for pair in pairs:
                pair = list(pair)

                raws1 = Answer_dict[pair[0]]['raws']
                raws2 = Answer_dict[pair[1]]['raws']
                
                for ra1, ra2 in zip(raws1, raws2):
                    site1 = ra1['site']
                    site2 = ra2['site']
                    inst1 = Answer_dict[pair[0]][site1]['oriInst']
                    inst2 = Answer_dict[pair[1]][site2]['oriInst']

                    if Secondary_filter(name[0], site1, inst1, ra1, site2, inst2, ra2) >= 3:
                        Inte_name.append(pair[0])
                        deleteList.append(pair[1])
                        for site_one in site:
                            if site_one in Answer_dict[pair[1]]:
                                if site_one in Answer_dict[pair[0]].keys() :                            
                                    Answer_dict[pair[0]][site_one]['A_id'].extend(Answer_dict[pair[1]][site_one]['A_id'])
                                    Answer_dict[pair[0]][site_one]['papers'].extend(Answer_dict[pair[1]][site_one]['papers'])
                                    Answer_dict[pair[0]]['raws'].extend(Answer_dict[pair[1]]['raws'])
                                    
                                    Answer_dict[pair[0]][site_one]['A_id'] = list(set(Answer_dict[pair[0]][site_one]['A_id']))
                                    Answer_dict[pair[0]][site_one]['papers'] = list(set(Answer_dict[pair[0]][site_one]['papers']))
                                else:
                                    Answer_dict[pair[0]][site_one] = Answer_dict[pair[1]][site_one]
   
                        flag = True
                        preprocessedList.remove(pair[1])
                        break
                if flag :
                    break
                
for d in deleteList:
    del Answer_dict[d]

for d in Answer_dict : 
    if 'raws' in Answer_dict[d] :
        del Answer_dict[d]['raws']

paper_site = ['KCI', 'SCIENCEON', 'DBPIA']

for check_name in set(Inte_name): #통합저자
    paper_check = {} #paper_id : title : co_author
    del_paper = [] #del paper list
    if check_name in Answer_dict.keys():
        for site_one in paper_site:
            if site_one in Answer_dict[check_name]:
                for raw_one in raw_dbs[site_one].find({"_id": {"$in": Answer_dict[check_name][site_one]['papers']}}):
                    if raw_one['title'] not in paper_check.keys(): #중복 title이 아니면
                        
                        for key_check in paper_check: #paper_chck에 있는 title과 유사도 비교
                            paper_sim = jaro.jaro_winkler_metric(key_check, raw_one['title'])
                            
                            if paper_sim >= 0.8: #유사도가 80% 이상이면
                                if paper_check[key_check]['co_author'] == raw_one['author'].split(';')[:-1]: #공동저자 비교
                                    if raw_one['_id'] in Answer_dict[check_name][site_one]['papers']:
                                        del_paper.append({raw_one['_id'] : raw_one['title']})
                                        Answer_dict[check_name][site_one]['papers'].remove(raw_one['_id'])
                                        del fp_dict[raw_one['_id']]
                                        break

                        paper_check[raw_one['title']] = {'paper_id' : raw_one['_id'], 'co_author' : raw_one['author'].split(';')[:-1]}
                        
                    else: #중복 title이면
                        del_paper.append({raw_one['_id'] : raw_one['title']})
                        Answer_dict[check_name][site_one]['papers'].remove(raw_one['_id'])
                        del fp_dict[raw_one['_id']]
                if Answer_dict[check_name][site_one]['papers'] == []: #site에 papers가 비어있으면 site 삭제
                    del Answer_dict[check_name][site_one]
                        
for fp in fp_dict:
    f_pyear = fc_simple_filter(fp_dict[fp]['year'], f_pyear)
    f_pinst = fc_simple_filter(fp_dict[fp]['inst'].replace(".", "^"), f_pinst)
    f_pjournal = fc_simple_filter(fp_dict[fp]['journal'].replace(".", "^"), f_pjournal)
    
filter_dict= {'keyId': keyid, 'fId': f_id, 'paper': { 
                'year': {'list': f_pyear, 'k': 'year', 'v': '연도' },
                'inst': {'k': 'inst', 'list': f_pinst, 'v': '소속', 'f': 'false' },
                'journal': {'list': f_pjournal, 'k': 'journal', 'v': '저널'}
            },
            'project': {
                'year': {'list': f_nyear, 'k': 'year', 'v': '연도' },
                'inst': {'list': f_ninst, 'k': 'inst', 'v': '소속' },
                'fund': {'k': 'fund', 'v': '과제수주비', 'list': f_nfund },
                'rsc': {'k': 'rsc', 'v': '참여인원', 'list': f_nrsc }
            }}


if len(Answer_dict) != 0:
    filters_category.insert_one(filter_dict)
    id_domestic.insert_many(Answer_dict.values()) #mongodb 추가
    analyzer = multicpu_220504.run_factor_integration(keyid, f_id)
    analyzer.run()
    print("Integration OK", time.time() - start1)
else:
    print("No Data")

os.system(f'python3 Filtering_en.py 0 {keyid}')