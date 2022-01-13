from pymongo import MongoClient
import itertools
import numpy as np
import jaro
import re
import sys

client = MongoClient('mongodb://203.255.92.141:27017', authSource='admin')
filter_info = client['PUBLIC']['FilterInfo'] #필터접근

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

mng_id = [] # Author id
paper = []

Answer_dict = {} # Answer result
site = ['SCIENCEON', 'NTIS', 'KCI']
fund = [0, 50000000, 100000000, 300000000, 500000000, 1000000000 ]
rsc = [0, 10, 30, 50, 100]

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

for i in range(len(key_querys)):
    mng_dict = {}
    for key_query in key_querys[i]: #rawdata(magid, paper) insert
        
        if site[i] == 'NTIS' :
            ori_inst = key_query['originalName']
            ntis_rsc = int(key_query['cntRscMan']) + int(key_query['cntRscWom'])
            ntis_fund = key_query['totalFund']
            ntis_year = key_query['prdStart'][:4]
            exi_inst = key_query['ldAgency']
            mng_name =  key_query['mng']
            mng_id = key_query['mngId']
            paper = [key_query['_id']]

            if simple_filter(ori_inst, ninst) and simple_filter(ntis_year, nyear) and complex_filter(ntis_fund, nfund, fund) and complex_filter(ntis_rsc, nrsc, rsc):
                if mng_id not in mng_dict:
                    mng_dict[mng_id] = {'name' : mng_name, 'inst' : exi_inst, 'papers' : [], 'oriInst' : ori_inst}
                mng_dict[mng_id]['papers'].append(paper)
                
        else:
            paper_year =  key_query['issue_year'][:4]
            paper_journal = key_query['journal']
            ori_inst = key_query['originalName'].split(';')[-2]
            paper_lang = key_query['issue_lang']
            exi_inst = key_query['author_inst'].split(';')[-2]
            mng_name = key_query['author'].split(';')[-2]
            mng_id = key_query['mngId']
            paper = [key_query['_id']]

            if simple_filter(paper_year, pyear) and simple_filter(paper_journal, pjournal) and simple_filter(ori_inst, pinst) and simple_filter(paper_lang, plang):
                if mng_id not in mng_dict:
                    mng_dict[mng_id] = {'name' : mng_name, 'inst' : exi_inst, 'papers' : [], 'oriInst' : ori_inst}
                mng_dict[mng_id]['papers'].append(paper)

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

                        else :# 사이트가 다를때 
                            if temp[key]['inst'] == exiinst  or (src != "" and src in tgt):  # 소속 같을때
                                Answer_dict[tempName][site[i]] =  {'inst' : exiinst, 'A_id': [mng_one], 'papers' : paper, 'oriInst' : oriinst}
                                if '대학교' in Answer_dict[tempName][site[i]]['oriInst'] and '대학교' not in Answer_dict[tempName]['inst']:
                                    Answer_dict[tempName]['inst'] = Answer_dict[tempName][site[i]]['oriInst']
                                flag = False
                                break
                            
                            elif mng_name+'_'+str(count+1) not in Answer_dict : #소속이 다를 때
                                Answer_dict[mng_name+'_'+str(count+1)] = Answer

                                if tempName == mng_name:
                                    Answer_dict[mng_name+'_0'] = temp
                                    del Answer_dict[mng_name]

                count += 1

def isEnglishOrKorean(input_s):
    k_count = 0
    e_count = 0
    for c in input_s:
        if ord('가') <= ord(c) <= ord('힣'):
            k_count+=1
        elif ord('a') <= ord(c.lower()) <= ord('z'):
            e_count+=1
    return "k" if k_count>1 else "e"

def check_college(univ0):
    branch_set = ['성균관대학교', '건국대학교', '한양대학교']
    univName = client['PUBLIC']['CollegeName']
    univ1 = re.sub("산학협력단|병원","",univ0)
    univ2 = re.sub("대학교","대학교 ",univ1)

    try:
        if isEnglishOrKorean(univ0) == 'e':
            univ0 = univ0.upper()
            univ0 = univ0.replace('.', ',')
            univ = univ0.split(', ')
        else:
            univ = univ2.replace(",", "").split()
            univ = list(set(univ))   
            
        for uni in univ:
            if uni in branch_set:
                if ("ERICA" or "에리카") in univ0:
                    univ[univ.index("한양대학교")] = "한양대학교(ERICA캠퍼스)"
                elif ("글로컬" or "GLOCAL") in univ0:
                    univ[univ.index("건국대학교")] = "건국대학교 GLOCAL(글로컬)캠퍼스"
                elif "자연과학캠퍼스" in univ0:
                    univ[univ.index("성균관대학교")] = "성균관대학교(자연과학캠퍼스)"

        univs = '{"$or": ['
        for u in range(len(univ)):
            if univ[-1] == univ[u]:
                univs += '{"inputName": "' + univ[u] + '"}'
            else:
                univs += '{"inputName": "' + univ[u] + '"}, '
        univs += ']}'

        univ_query = univName.find_one(eval(univs))

        if univ_query is None:
            print("Search inst None")
            return univ0, False
        else:
            return univ_query['originalName'], True 
        
    except SyntaxError as e:
        print(e)
        print(univ0)
        return univ0, False

def filter(name, site, inst, rawdata):
    instbool = False
    if site == 'NTIS' :
        inst, instbool = check_college(inst)
        coauthor = rawdata['rsc'].split(";")
        year = int(rawdata['prdStart'][:4])
        keyword = rawdata['koKeyword'].split(",")
        journal = ""
        conference = ""
        title = ""

    else :
        inst, instbool = check_college(inst)
        coauthor = rawdata['author'].split(";")[1:-1]
        year = int(rawdata['issue_year'][:4])
        
        if rawdata['paper_keyword'] == [] or rawdata['paper_keyword'] == 'None':
            keyword = []
        elif len(rawdata['paper_keyword']) > 1:
            for i in range(0, len(rawdata['paper_keyword'])):
                keyword = []
                keyword.append(rawdata['paper_keyword'][i].replace(" ", "").split("."))            
        else:
            keyword = rawdata['paper_keyword'].replace(" ", "").split(".")

        journal = rawdata['journal']
        conference = rawdata['issue_inst']
        title = rawdata['title']

    return inst, coauthor, year, keyword, journal, conference, title, instbool

def Secondary_filter(name, site1, inst1, raw_one1, site2, inst2, raw_one2):
    inst = 0
    inst1, coauthor1, year1, keyword1, journal1, conference1, title1, instbool1 = filter(name, site1, inst1, raw_one1)
    inst2, coauthor2, year2, keyword2, journal2, conference2, title2, instbool2 = filter(name, site2, inst2, raw_one2)

    if instbool1 and instbool2:
        if inst1 == inst2:
            inst = 1
    else:
        inst = jaro.jaro_winkler_metric(inst1, inst2)
        
    weight = 0
    joc = 0
    
    if name in coauthor1:
        coauthor1.remove(name)

    if name in coauthor2:
        coauthor2.remove(name)
    
    co_author_count = len([i for i in coauthor1 if i in coauthor2])

    if site1 != 'NTIS' and site2 != 'NTIS' :
        if title1 == title2 or inst >= 0.8: #or mng1 == mng2:
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

    #print(f'joc: {joc} | yop: {yop} | co_authorship: {co_authorship} | keyword: {keyword}')

    weight = joc + yop + co_authorship + keyword

    return weight

data0 = Answer_dict
raw_dbs = {'NTIS' : ntis_raw, 'SCIENCEON' : scion_raw, 'KCI' : kci_raw}

def getRaw(name):
    if 'raws' not in data0[name]:
        raws = []
        for site_one in site:
            if site_one in data0[name]:
                for c in raw_dbs[site_one].find({"_id": {"$in": data0[name][site_one]['papers']}}):
                    c['site'] = site_one
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
            
            for pair in pairs:
                pair = list(pair)

                raws1 = data0[pair[0]]['raws']
                raws2 = data0[pair[1]]['raws']
                
                for ra1, ra2 in zip(raws1, raws2):
                    site1 = ra1['site']
                    site2 = ra2['site']
                    inst1 = data0[pair[0]][site1]['inst']
                    inst2 = data0[pair[1]][site2]['inst']

                    if Secondary_filter(name[0], site1, inst1, ra1, site2, inst2, ra2) >= 3:
                        deleteList.append(pair[1])
                        for site_one in site:
                            if site_one in data0[pair[1]]:
                                if site_one in data0[pair[0]].keys() :                            
                                    data0[pair[0]][site_one]['A_id'].extend(data0[pair[1]][site_one]['A_id'])
                                    data0[pair[0]][site_one]['papers'].extend(data0[pair[1]][site_one]['papers'])
                                    data0[pair[0]]['raws'].extend(data0[pair[1]]['raws'])
                                else:
                                    data0[pair[0]][site_one] = data0[pair[1]][site_one]
                        flag = True
                        preprocessedList.remove(pair[1])
                        break
                if flag :
                    break
         
for d in deleteList:
    del data0[d]

for d in data0 : 
    if 'raws' in data0[d] :
        del data0[d]['raws']

id_domestic.insert_many(data0.values()) #mongodb 추가
print("Integration OK")
