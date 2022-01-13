import sys
from pymongo import MongoClient

client = MongoClient('mongodb://203.255.92.141:27017', authSource='admin')
filter_info = client['PUBLIC']['FilterInfo'] #필터접근

f_id = int(sys.argv[1]) #input
keyid = int(sys.argv[2])  #keyid

nyear = []
pyear = []

fid_key_query = filter_info.find_one({'fId' : f_id }) #필터검색

if  f_id > 0 and fid_key_query != None: #f_id check
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

scion_autpaper = client['SCIENCEON']['AuthorPapers']
ntis_autpaper = client['NTIS']['AuthorPapers']
kci_autpaper = client['KCI']['AuthorPapers']

scion_raw = client['SCIENCEON']['Rawdata']
ntis_raw = client['NTIS']['Rawdata']
kci_raw = client['KCI']['Rawdata']

scion_key_query = scion_raw.find({ 'keyId' : keyid })
ntis_key_query = ntis_raw.find({ 'keyId' : keyid })
kci_key_query = kci_raw.find({ 'keyId' : keyid })

auts = [scion_aut, ntis_aut, kci_aut] #저자 이름, 소속
key_querys = [scion_key_query, ntis_key_query, kci_key_query] #a_id

A_id = []
all_name = []
all_inst = []
Answer_dict = {} #통합결과

site = ['Scienceon', 'NTIS', 'KCI']
savetime = 0

#print(scion_autpaper.count_documents({ 'keyId' : keyid }) + ntis_autpaper.count_documents({ 'keyId' : keyid }) + kci_autpaper.count_documents({ 'keyId' : keyid }))
fund = [0, 50000000, 100000000, 300000000, 500000000, 1000000000 ]
rsc = [0, 10, 30, 50, 100]

def simple_filter(value, filetrs) :
    if value in filetrs or filetrs == []:
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
    mngid_dict = {} #mngid, paper value dict / site마다

    for key_query in key_querys[i]: #rawdata(magid, paper) insert

        if f_id < 1 and fid_key_query == None:
            if key_query['mngId'] not in mngid_dict:
                mngid_dict[key_query['mngId']] = []
            mngid_dict[key_query['mngId']].append(key_query['_id'])

        elif site[i] == 'NTIS' :
            ntis_inst = key_query['ldAgency']
            ntis_rsc = int(key_query['cntRscMan']) + int(key_query['cntRscWom'])
            ntis_fund = key_query['totalFund']
            ntis_year = key_query['prdStart'][:4]
            #print(site[i], ntis_inst, ntis_rsc, ntis_fund)
            
            #if ntis_filter(ntis_inst, ntis_fund, ntis_year, ntis_rsc): #필터링
            if simple_filter(ntis_inst, ninst) and simple_filter(ntis_year, nyear) and complex_filter(ntis_fund, nfund, fund) and complex_filter(ntis_rsc, nrsc, rsc):
                if key_query['mngId'] not in mngid_dict:
                    mngid_dict[key_query['mngId']] = []
                mngid_dict[key_query['mngId']].append(key_query['_id'])

        else:
            paper_year =  key_query['issue_year'][:4]
            paper_journal = key_query['journal']
            paper_inst = key_query['author_inst'].split(';')[0]
            paper_lang = key_query['issue_lang']
            # print(site[i], paper_inst, paper_journal, paper_lang)
            if simple_filter(paper_year, pyear) and simple_filter(paper_journal, pjournal) and simple_filter(paper_inst, pinst) and simple_filter(paper_lang, plang):
            #if paper_filter(paper_year, paper_journal, paper_inst, paper_lang): #필터링
                if key_query['mngId'] not in mngid_dict:
                    mngid_dict[key_query['mngId']] = []
                mngid_dict[key_query['mngId']].append(key_query['_id'])
    #print(mngid_dict)
    paper = []
    aut_querys = auts[i].find({'_id': { '$in' : list(mngid_dict.keys())}})
    for aut_query in aut_querys :
        A_id = aut_query['_id']
        paper = mngid_dict[A_id]
        all_name.append(aut_query['name'])
        all_inst.append(aut_query['inst'].replace("(주) ", "").replace("(주)", "").split(' ')[0])

        Answer = {'fid': f_id, 'keyId': keyid, 'name' : all_name[-1], site[i] : {'inst' : all_inst[-1], 'A_id': A_id, 'papers' : paper} }

        if all_name[-1] not in Answer_dict and all_name[-1]+'_0' not in Answer_dict : #동명이인이 없을 때
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
                                Answer_dict[tempName][site[i]]['papers'].extend(paper)
                                flag = False
                                break

                            elif all_name[-1]+'_'+str(count+1) not in Answer_dict : #소속이 다를 때
                                Answer_dict[all_name[-1]+'_'+str(count+1)] = Answer

                                if tempName == all_name[-1]:
                                    Answer_dict[all_name[-1]+'_0'] = temp
                                    del Answer_dict[all_name[-1]]

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

                count += 1

#print(sorted(Answer_dict.items()))

id_domestic = client['ID']['Domestic']
id_domestic.insert_many(Answer_dict.values())
print("Integration OK")
