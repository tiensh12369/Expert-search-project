from email.mime import base
import re, math,threading, datetime
from typing import List
import pymongo
from sklearn.feature_extraction.text import TfidfVectorizer
from multiprocessing import Pool
from pymongo import MongoClient
from numpy.linalg import norm
from threading import Thread
import scipy.sparse as sp
from time import sleep
from numpy import dot
import numpy as np


class factor_integration(threading.Thread):
    # 쓰레드 상속 >> run에서 사용
    def __init__(self, start, num_data, fid, keyID):
        threading.Thread.__init__(self)
        # 받아오는 값
        self.keyId       = keyID
        self.fid         = fid
        self.start_index = start # 시작위치
        self.end         = num_data # 총 데이터 >> 이걸 100개씩 나눠서 실행시켜야함.

        self.client         = MongoClient('203.255.92.141:27017', connect=False)
        self.ID             = self.client['ID']
        self.PUBLIC         = self.client['PUBLIC']
        self.new_max_factor = self.PUBLIC['new_factor'] 
        self.ntis_client    = self.client['NTIS']
        self.scienceon      = self.client['SCIENCEON']
        self.KCI_main       = self.client['KCI']

        # DBPIA 추가
        self.DBPIA          = self.client['DBPIA']
        

        self.KCI  = self.client.PUBLIC.KCI
        self.SCI  = self.client.PUBLIC.SCI
        self.kDic = {}
        self.sDic = {}
        for doc in self.KCI.find({}) :
            self.kDic[doc['name']] = doc['IF']
        for doc in self.SCI.find({}) :
            self.sDic[doc['name']] = doc['IF']

    def run(self):
        all_count   = self.end - self.start_index
        dataPerPage = 100
        allPage     = math.ceil(all_count/dataPerPage)
        for i in range(allPage):
            
            contrib = []
            qual    = []
            sCount  = self.start_index + (i*dataPerPage)
            lCoount = min(dataPerPage, self.end - sCount )
            data, object_data, base_data = self.getBackdata(sCount, lCoount, self.fid, self.keyId)

            # (pYears, keywords, _ntisQtyBackdata, _ntisContBackdata, _ntisCoopBackdata, _sconQtyBackdata, _sconContBackdata, _sconCoopBackdata,_KCIconQtyBackdata, _KCIContBackdata, _KCICoopBackdata, querykey, numProjects_list, numPapers_list, totalcitation_list, recentYear_list, totalcoop_list, coopList) = self.getRawBackdata(data,self.keyId, object_data)
            (pYears, keywords, _ntisQtyBackdata, _ntisContBackdata, _ntisCoopBackdata) = self.getRawBackdata(data,self.keyId, object_data)['NTIS']
            (_sconQtyBackdata, _sconContBackdata, _sconCoopBackdata)                   = self.getRawBackdata(data,self.keyId, object_data)['SCIENCEON']
            (_KCIconQtyBackdata, _KCIContBackdata, _KCICoopBackdata)                   = self.getRawBackdata(data,self.keyId, object_data)['KCI']
            (_DBPIAconQtyBackdata, _DBPIAContBackdata, _DBPIACoopBackdata)             = self.getRawBackdata(data,self.keyId, object_data)['DBPIA']
            (querykey, numProjects_list, numPapers_list, totalcitation_list, recentYear_list, totalcoop_list, coopList) = self.getRawBackdata(data,self.keyId, object_data)['etc']

            # for k in range(len(self.scoquality(_sconQtyBackdata))):
            #     qual.append(self.ntisquality(_ntisQtyBackdata)[k]+self.scoquality(_sconQtyBackdata)[k]+self.scoquality(_KCIconQtyBackdata)[k] + self.scoquality(_DBPIAconQtyBackdata)[k])

            for k in range(len(self.scoquality(_sconQtyBackdata))):
                qual.append([self.ntisquality(_ntisQtyBackdata)[k], self.scoquality(_sconQtyBackdata)[k] + self.scoquality(_KCIconQtyBackdata)[k] + self.scoquality(_DBPIAconQtyBackdata)[k]])

            for j in range(len(self.scocont(_sconContBackdata))):
                contrib.append(self.ntiscont(_ntisContBackdata)[j]+self.scocont(_sconContBackdata)[j]+self.scocont(_KCIContBackdata)[j] + self.scocont(_DBPIAContBackdata)[j])

            # # coop = []
            # # scoop = self.coop(_sconCoopBackdata)
            # # kcoop = self.coop(_KCICoopBackdata)
            # # for x in range(len(_sconCoopBackdata)):
            # #     coop.append(scoop[x] + kcoop[x])

            contBit  = [1 if y > 0 else y for y in contrib]

            accuracy = self.acc(keywords, contBit, querykey)
        
            recentness, lct_list = self.recentness(pYears)
            
            self.insert_max_factor(qual, accuracy, totalcoop_list, recentness,self.keyId)
            
            real_final_last_data = []            
            count_base_data      = 0
            # print(base_data)
            # print(qual)
            for doc1 in base_data:
                doc1['numProjects']   = numProjects_list[count_base_data]
                doc1['numPapers']     = numPapers_list[count_base_data]
                doc1['totalCitation'] = totalcitation_list[count_base_data]
                doc1['recentYear']    = recentYear_list[count_base_data]
                doc1['totalCoop']     = totalcoop_list[count_base_data]
                doc1['score']         = 0
                doc1['coopList']      = coopList[count_base_data]
                
                factor                = {}
                # factor['qual']        = qual[count_base_data]
                factor['ntisQual']    = qual[count_base_data][0]
                factor['remainQual']  = qual[count_base_data][1]
                factor['lct']         = lct_list[count_base_data] / 2 ## 최신성 최대 0.5점
                factor['acc']         = accuracy[count_base_data]
                factor['coop']        = totalcoop_list[count_base_data]
                factor['qunt']        = recentness[count_base_data] # 정규화 후 
                doc1['factor']        = factor
                count_base_data      += 1
                real_final_last_data.append(doc1)
                # self.ID['test'].insert_one(doc1)
                # self.ID['test'].insert_one(real_final_last_data)

            #try:
            #    self.ID['test'].insert_many(real_final_last_data)
            #except Exception as e:
            #    print(e)
            #    continue

            self.ID['test'].insert_many(real_final_last_data)


    def insert_max_factor(self, qual, accuracy, coop, pYears, keyID):
        ntisQual   = max(map(lambda x:x[0], qual))
        remainQual = max(map(lambda x:x[1], qual))
        accuracy   = max(accuracy)
        coop       = max(coop)        
        recentness = max(pYears)
        keyId      = keyID
        # maxFactors = {'keyId': self.keyId, 'Quality' : qual, 'accuracy' : accuracy, 'recentness' : recentness, 'coop': coop }

        # 정규화를 하기 위한 각 factor당 max값을 넣어줌.
        # self.new_max_factor.update_one({"keyId" : keyId}, {'$max':{"Quality":qual, "accuracy":accuracy , "recentness":recentness , "coop":coop }})
        self.new_max_factor.update_one({"keyId" : keyId}, {'$max':{"ntisQual":ntisQual, "remainQual" : remainQual, "accuracy":accuracy , "recentness":recentness , "coop":coop }})
       
    def getBackdata(self, i, dataPerPage, fid, keyID):
        self.keyID    = keyID
        
        objectid_data = []   
        getBackdata   = []
        base_data     = self.ID['Domestic'].find({"keyId":keyID, "fid":fid}).skip(i).limit(dataPerPage)
        base_data1    = [] 
        for doc in base_data: 
            base_data1.append(doc)
            papersNumber   = 0
            getBackdataDic = {}
            objectid_data.insert(0,(doc['_id']))
            if ("NTIS" in doc):
                getBackdataDic['ntis'] = doc['NTIS']['A_id']
                getBackdataDic['ntis papers'] = doc['NTIS']['papers']
                papersNumber += len(doc['NTIS']['papers'])
            else:
                getBackdataDic['ntis'] = None
                getBackdataDic['ntis papers'] = []
                        
            if ("SCIENCEON" in doc):
                getBackdataDic['scienceon'] = doc['SCIENCEON']['A_id']
                getBackdataDic['scienceon papers'] = doc['SCIENCEON']['papers']
                papersNumber += len(doc['SCIENCEON']['papers'])
            else:
                getBackdataDic['scienceon'] = None
                getBackdataDic['scienceon papers'] = []

            if ("KCI" in doc):
                getBackdataDic['KCI'] = doc['KCI']['A_id']
                getBackdataDic['KCI papers'] = doc['KCI']['papers']
                papersNumber += len(doc['KCI']['papers'])
            else:
                getBackdataDic['KCI'] = None
                getBackdataDic['KCI papers'] = []    
            
            # DBPIA 추가
            if ("DBPIA" in doc):
                getBackdataDic['DBPIA'] = doc['DBPIA']['A_id']
                getBackdataDic['DBPIA papers'] = doc['DBPIA']['papers']
                papersNumber += len(doc['DBPIA']['papers'])
            else:
                getBackdataDic['DBPIA'] = None
                getBackdataDic['DBPIA papers'] = []

            getBackdataDic['number'] = papersNumber
            getBackdata.append(getBackdataDic)
        return  getBackdata, objectid_data, base_data1
        
    def getRawBackdata(self, getBackdata, keyID, object_data):

        pYears       = [] #NTIS & SCIENCEON & KCI
        keywords     = [] #NTIS & SCIENCEON & KCI
        qty          = [] #NTIS & SCIENCEON
        totalFunds   = [] #NTIS
        mngIds       = [] #NTIS
        ntis_id      = [] #NTIS
        authorInsts1 = [] #SCIENCEON
        authors1     = [] #SCIENCEON
        issueInsts1  = [] #SCIENCEON
        issueLangs1  = [] #SCIENCEON
        citation1    = [] #SCIENCEON
        scienceon_id = [] #SCIENCEON
        authorInsts2 = [] #KCI
        authors2     = [] #KCI
        issueInsts2  = [] #KCI
        issueLangs2  = [] #KCI
        citation2    = [] #KCI
        KCI_id       = [] #KCI

        # DBPIA 추가
        authorInsts3 = [] #DBPIA
        authors3     = [] #DBPIA
        issueInsts3  = [] #DBPIA
        issueLangs3  = [] #DBPIA
        citation3    = [] #DBPIA
        DBPIA_id     = [] #DBPIA
        



        querykey     = []
        
        totalcitation_list = []
        recentYear_list    = []
        totalcoop_list     = []
        numPapers_list     = []
        numProjects_list   = []
        coopList           = []
        for i in range(len(getBackdata) - 1, -1, -1):
            coopname      = []
            totalcitation = 0
            recentYear    = []  
            totalcoop     = 0   #공동연구
            numPapers     = 0  # 논문수 > sci, kci
            numProjects   = 0 # 프로젝트 > ntis
            
            _pYear        = [] #NTIS & SCIENCEON & KCI
            _keywords     = [] #NTIS & SCIENCEON & KCI
            
            fund_list     = [] #NTIS
            _mngIds       = [] #NTIS
            __keyword     = [] #NTIS
            
            _keyword1     = [] #SCIENCEON
            _authorInsts1 = [] #SCIENCEON
            _authors1     = [] #SCIENCEON
            _issueInsts1  = [] #SCIENCEON
            _issueLangs1  = [] #SCIENCEON
            _citation1    = [] #SCIENCEON
            _scienceon_id = [] #SCIENCEON
            _keyword2     = [] #KCI
            _authorInsts2 = [] #KCI
            _authors2     = [] #KCI
            _issueInsts2  = [] #KCI
            _issueLangs2  = [] #KCI
            _citation2    = [] #KCI
            _KCI_id       = [] #KCI
            _citation     = []

            # DBPIA 추가
            _keyword3     = [] #DBPIA
            _authorInsts3 = [] #DBPIA
            _authors3     = [] #DBPIA
            _issueInsts3  = [] #DBPIA
            _issueLangs3  = [] #DBPIA
            _citation3    = [] #DBPIA


            #NTIS
            if (getBackdata[i]['ntis'] != None):
                
                ntis_id.insert(0,getBackdata[i]['ntis'])
                for doc in self.ntis_client['Rawdata'].find({"keyId": keyID, "_id": {"$in" : getBackdata[i]['ntis papers']}}):
                    numProjects += 1
                    
                    fund_list.append(math.log(float(doc['totalFund'])+1))
                    _mngIds.append(doc['mngId'])
                    for j in doc['qryKeyword']:
                        if j not in querykey:
                            querykey.append(j)
                        
                    if doc['prdEnd'] != 'null':
                        _pYear.append(int(doc['prdEnd'][0:4]))
                    elif (doc['prdEnd'] == 'null') and (doc['prdStart'] != 'null'):
                        _pYear.append(int(doc['prdStart'][0:4]))
                    else:
                        _pYear.append(int(2000))
                        
                    __keyword.append(doc['koTitle'])
                    __keyword.append(doc['enTitle'])
                    __keyword.append(doc['koKeyword'])
                    __keyword.append(doc['enKeyword'])

                if len(__keyword) != 0:
                    _keywords.insert(0,__keyword)
                    totalFunds.insert(0,sum(fund_list))
                    mngIds.insert(0,_mngIds)
            else:
                ntis_id.insert(0,None)
                totalFunds.insert(0,0)
                mngIds.insert(0,_mngIds)

            #SCIENCEON
            if (getBackdata[i]['scienceon'] != None):
                
                scienceon_id.insert(0,getBackdata[i]['scienceon'])

                for doc in self.scienceon['Rawdata'].find({"keyId": keyID, "_id": {"$in" : getBackdata[i]['scienceon papers']}}):
                    originalName = doc['originalName']
                    originalName1 = originalName.split(';')
                    
                    pcnt = len(originalName1)-1
                    cnt = 0
                    for n in range(pcnt):
                        if True == self.check_college(originalName1[n]):
                            cnt += 1
                        else:    
                            coopname.append(originalName1[n])
                            
                    if cnt != pcnt and cnt >= 1:
                        totalcoop += 1
                    for j in doc['qryKeyword']:
                        if j not in querykey:
                            querykey.append(j)
                    _keyword1.append(doc['title'])
                    _keyword1.append(doc['english_title'])
                    _keyword1.append(doc['paper_keyword'])
                    _keyword1.append(doc['abstract'])
                    _keyword1.append(doc['english_abstract'])
                    _pYear.append(int(doc['issue_year'][0:4]))
                    recentYear.append(int(doc['issue_year'][0:4]))
                    _authorInsts1.append(doc['author_inst'])
                    _authors1.append(doc['author_id']) #= doc['author_id'].split(';')
                    _issueInsts1.append(doc['issue_inst'])
                    _issueLangs1.append(doc['issue_lang'])

                    _citation1.append(int(doc['citation']))
                    totalcitation += int(doc['citation'])
                    numPapers += 1
                        
                if len(_keyword1) != 0 :
                    authorInsts1.insert(0,_authorInsts1)
                    authors1.insert(0,_authors1)
                    issueInsts1.insert(0, _issueInsts1)
                    _keywords.insert(0,_keyword1)
                    issueLangs1.insert(0,_issueLangs1)
                    citation1.insert(0,_citation1)
            else:
                issueInsts1.insert(0,_issueInsts1)
                issueLangs1.insert(0,_issueLangs1)
                citation1.insert(0,_citation1)
                authors1.insert(0,"scienceon"+str(i))
                scienceon_id.insert(0,"sco"+str(i))
                authorInsts1.insert(0,_authorInsts1)

            # KCI
            if (getBackdata[i]['KCI'] != None):
                
                KCI_id.insert(0,getBackdata[i]['KCI'])
                for doc in self.KCI_main['Rawdata'].find({"keyId": keyID, "_id": {"$in" : getBackdata[i]['KCI papers']}}):
                    numPapers += 1
                    originalName = doc['originalName']
                    originalName2 = originalName.split(';')
                   
                    
                    pcnt = len(originalName2)-1
                    cnt = 0
                    for m in range(pcnt):
                        if True == self.check_college(originalName2[m]):
                            cnt += 1    
                        else:   
                            coopname.append(originalName2[m])
                            
                    if cnt != pcnt and cnt >= 1:
                        totalcoop += 1
                    
                    _keyword2.append(doc['title'])
                    _keyword2.append(doc['english_title'])
                    _keyword2.append(doc['paper_keyword'])
                    _keyword2.append(doc['abstract'])
                    _keyword2.append(doc['english_abstract'])
                    _pYear.append(int(doc['issue_year'][0:4]))
                    recentYear.append(int(doc['issue_year'][0:4]))
                    _authorInsts2.append(doc['author_inst'])
                    _authors2.append(doc['author_id']) #= doc['author_id'].split(';')
                    _issueInsts2.append(doc['issue_inst'])
                    _issueLangs2.append(doc['issue_lang'])
                    _citation2.append(int(doc['citation']))
                    totalcitation += int(doc['citation'])
            
                        
                if len(_keyword2) != 0 :
                    authorInsts2.insert(0,_authorInsts2)
                    authors2.insert(0,_authors2)
                    issueInsts2.insert(0, _issueInsts2)
                    _keywords.insert(0,_keyword2)
                    issueLangs2.insert(0,_issueLangs2)
                    citation2.insert(0,_citation2)
                
            else:
                issueInsts2.insert(0,_issueInsts2)
                issueLangs2.insert(0,_issueLangs2)
                citation2.insert(0,_citation2)
                authors2.insert(0,"kci"+str(i))
                KCI_id.insert(0,"kci"+str(i))
                authorInsts2.insert(0,_authorInsts2)

            # DBPIA 추가
            if (getBackdata[i]['DBPIA'] != None):
                DBPIA_id.insert(0, getBackdata[i]['DBPIA'])

                for doc in self.DBPIA['Rawdata'].find({"keyId": keyID, "_id": {"$in" : getBackdata[i]['DBPIA papers']}}):
                    numPapers += 1
                    
                    # DBPIA는 사실 title만 넣으면 됌..
                    _keyword3.append(doc['title'])
                    #_keyword3.extend([doc['title'],doc['english_title'],doc['paper_keyword'],doc['abstract'],doc['english_abstract']])
                    #_keyword3.extend([doc['title'],doc['title'], doc['title'],doc['title'],doc['title']])
                    
                    year = int(doc['issue_year'][:4])
                    _pYear.append(year)
                    recentYear.append(year)

                    for j in doc['qryKeyword']:
                        if j not in querykey:
                            querykey.append(j)
                    #for author_id in doc['author_id'].split(';'):
                    #    author_info = self.DBPIA['Author'].find({'_id' : author_id })
                    #    _authors3.append(author_info['_id'])
                    #    if author_info['hasInst'] == True:
                    #        _authorInsts3.append(author_info['inst'])

                    _issueInsts3.append(doc['issue_inst'])
                    _issueLangs3.append(doc['issue_lang'])
                    _citation3.append(int(doc['citation']))
                    totalcitation += int(doc['citation'])

                if len(_keyword3) != 0 :
                    authorInsts3.insert(0,_authorInsts3)
                    authors3.insert(0,_authors3)
                    issueInsts3.insert(0, _issueInsts3)
                    _keywords.insert(0,_keyword3)
                    issueLangs3.insert(0,_issueLangs3)
                    citation3.insert(0,_citation3)

            else:
                issueInsts3.insert(0,_issueInsts3)
                issueLangs3.insert(0,_issueLangs3)
                citation3.insert(0,_citation3)
                authors3.insert(0,"dbpia"+str(i))
                DBPIA_id.insert(0,"dbpia"+str(i))
                authorInsts3.insert(0,_authorInsts3)


            totalcoop_list.insert(0,totalcoop) #1
            coopname = list(set(coopname))
            for num, q in enumerate(coopname):
                if coopname[num] == "":
                    coopname.pop(num)
            coopList.insert(0, coopname)
            
            if recentYear == []:
                recentYear_list.insert(0,0)
            else:
                recentYear_list.insert(0,max(recentYear)) #2
            totalcitation_list.insert(0,totalcitation) #3
            numPapers_list.insert(0,numPapers) #4
            numProjects_list.insert(0, numProjects) #5
            pYears.insert(0,_pYear)
            keywords.insert(0,_keywords)
            
            # NTIS, SCIENCEON, KCI, DBPIA, etc
        # return pYears, keywords, totalFunds, {'mngIds' : mngIds, 'A_ID' : ntis_id}, None, \
        #     {'issueInsts' : issueInsts1, 'issueLangs' : issueLangs1, 'citation' : citation1}, {'authors' : authors1, 'A_ID' : scienceon_id  }, authorInsts1, \
        #     {'issueInsts' : issueInsts2, 'issueLangs' : issueLangs2, 'citation' : citation2}, {'authors' : authors2, 'A_ID' : KCI_id  }, authorInsts2, \
        #     {'issueInsts' : issueInsts3, 'issueLangs' : issueLangs3, 'citation' : citation3}, {'authors' : authors3, 'A_ID' : KCI_id  }, authorInsts3, \
        #     querykey, numProjects_list, numPapers_list, totalcitation_list, recentYear_list, totalcoop_list, coopList
        return {'NTIS' : [pYears, keywords, totalFunds, {'mngIds' : mngIds, 'A_ID' : ntis_id}, None], 
        'SCIENCEON' : [{'issueInsts' : issueInsts1, 'issueLangs' : issueLangs1, 'citation' : citation1}, {'authors' : authors1, 'A_ID' : scienceon_id  }, authorInsts1], 
        'KCI' : [{'issueInsts' : issueInsts2, 'issueLangs' : issueLangs2, 'citation' : citation2}, {'authors' : authors2, 'A_ID' : KCI_id  }, authorInsts2], 
        'DBPIA' : [{'issueInsts' : issueInsts3, 'issueLangs' : issueLangs3, 'citation' : citation3}, {'authors' : authors3, 'A_ID' : KCI_id  }, authorInsts3], 
        'etc' : [querykey, numProjects_list, numPapers_list, totalcitation_list, recentYear_list, totalcoop_list, coopList]}

    def recentness(self, pYears):
        dt = datetime.datetime.now()
        rct_list = []
        lct_list = []
        for i in range(len(pYears)):
            rct = 0
            lct = 0
            try:
                year_avg = sum(pYears[i]) / len(pYears[i])
                if year_avg >= int(dt.year)-2: # 최신년도 기준으로 과거 2년까지 +1점
                    lct = 1
                elif int(dt.year)-15 < year_avg <= int(dt.year)-3: # 최신년도 기준 과거 15년 ~ 과거 2년까지 
                    lct = max(round((1-(int(dt.year)-3-year_avg)*0.1),2), 0)
               
            except Exception as e:
                rct_list.append(0)
                lct_list.append(0)
                continue

            for j in range(len(pYears[i])):
                if (year_avg - 5 < pYears[i][j] < year_avg + 5):
                    rct += 1
                
            if len(pYears[i]) != 0:
                rct_list.append(rct)
                lct_list.append(lct)
            else:
                rct_list.append(0)
                lct_list.append(0)

        return rct_list, lct_list # rct는 정규화를 해야하고,  // lct는 절대적인 값이므로 그냥 더함. 앞에꺼

    
    def coop(self, _coopBackdata):
        """
        @ Method Name     : coop
        @ Method explain  : 협업도 계산 함수
        @ _contBackdata   : getRawBackdata 함수에서 mngIds, A_ID 값을 가지고 있는 변수
        """
        oemList = ["Hyundai", "Kia","Toyota","Honda","Nissan","General Motors", "Chevrolet","Ford motor", "Volkswagen", "Audi", "BMW", "Bayerische Motoren Werke", "Mercedes-Benz", "daimler", "Volvo", "Renault", "Jaguar", "Acura", "Mazda", "Subaru", "Suzuki", "Isuzu","Daihatsu","Peugeot","Mclaren", "Bugatti", "Rolls Royce", "Bentley", "Aston Martin", "Land Rover", "Lotus","Lexus",   "Infiniti", "Datson", "Mitsubishi", "Mitsuoka","Great Wall","Cadillac", "Tesla", "Jeep", "Dodge", "Chrysler","Porsche", "Opel", "Borgward", "Gumfut", "FIAT", "Ferrari", "Lamborghini", "Maserati","Peugeot"]
        score = []
        for i in range(len(_coopBackdata)):
            point = 0
            for insts in _coopBackdata[i]:
                if insts != None :
                    for oem in oemList :
                        if oem in insts:
                            point = point + 1
                            break
            score.append(point)
        return score
    
    def ntiscont(self, _contBackdata):
        mngIds = _contBackdata['mngIds']
        A_ID   = _contBackdata['A_ID']
        point  = []
        for i in range(len(mngIds)):
            pt = 0
            temp = 0
            for k in range(len(mngIds[i])):
                if mngIds[i][k] != None:
                    if A_ID[i][0] == mngIds[i][k] :
                        pt += 10
                    else:
                        temp += 1
            if pt > 0 : 
                pt += temp
            point.append(pt)
        return point
    
    def scocont(self, _contBackdata):
        authors = _contBackdata['authors']
        A_ID = _contBackdata['A_ID']
        aidToDict = {}
        for i in A_ID:
            if type(i) == list:
                i = i[0]         
            aidToDict[i] = 0
        for num, i in enumerate(authors):
            if type(i) != list:
                a = [1]
                a[0] = i
                authors[num] = a
              
        for i in range(len(authors)):
            for u in range(len(authors[i])):

                x = authors[i][u].split(';')
             
                for author in enumerate(x):
                    quest = author[1] in aidToDict
                   
                    if author[1] in aidToDict and author[1] == A_ID[i]:
                  
                        if author[0] == 0:
                            aidToDict[author[1]] += 1.0
                        elif author[0] == len(x)-1:
                            aidToDict[author[1]] += 3.0
                        else :
                            aidToDict[author[1]] += ((author[0]+1)/len(x))
     
        return list(aidToDict.values())

    def ntisquality(self, totalFunds):
        # return math.log(totalFunds)
        return [ math.log(_) if _ != 0 else 0 for _ in totalFunds ]
    
    def scoquality(self, _qtyBackdata):
        issueInsts = _qtyBackdata['issueInsts']
        issueLangs = _qtyBackdata['issueLangs']
        citation   = _qtyBackdata['citation']

        IF = []
        for i in range(len(issueInsts)):
            tempIF = 0
            for j in range(len(issueInsts[i])):
                temp = None
                tempIFIF = 0
                n = 1
                if issueLangs[i][j] == 'kor':
                    if isinstance(issueInsts[i][j], str) :
                        tempIFIF = self.kDic.get(issueInsts[i][j],0)
                else:
                    if isinstance(issueInsts[i][j], str) :
                        tempIFIF = self.sDic.get(issueInsts[i][j],0)
                    n = 3

                tempIF += math.log(((citation[i][j]*n)+1) * (tempIFIF+1.1))
            IF.append(tempIF)
        return IF

    
    def acc(self, keywords, contBit, querykey):
        rtv = contBit.copy()
        for i in range(len(keywords)):
           
            if rtv[i] != 0:
                temp = calAcc(keywords[i], querykey)
                if temp == 0.0 :
                    rtv[i] = 0.02 
                else :
                    rtv[i] = temp
            """
            except Exception as e :
                print(keywords[i])
                print(e)
            """
        return rtv


    def isEnglishOrKorean(self, input_s):
        k_count = 0
        e_count = 0
        for c in input_s:
            if ord('가') <= ord(c) <= ord('힣'):
                k_count+=1
            elif ord('a') <= ord(c.lower()) <= ord('z'):
                e_count+=1
        return "k" if k_count>1 else "e"

    def check_college(self, univ0):
        branch_set = ['성균관대학교', '건국대학교', '한양대학교']
        univName = self.client['PUBLIC']['CollegeName']
        univ1 = re.sub("산학협력단|병원","",str(univ0))
        univ2 = re.sub("대학교","대학교 ",str(univ1))

        try:
            if self.isEnglishOrKorean(str(univ0)) == 'e':
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
                return False
            else:
                return True #univ0, univ_query
            
        except SyntaxError as e:
            print(e)
            return False


def calAcc(keywords, querykey):
    flat_list = []
    for sublist in keywords :
        for item in sublist :
            if item is not None and item != 'None' and item != "" and isinstance(item, str) :
                flat_list.append(item)
    if len(flat_list) == 0 :
        return 0 
    # print(flat_list)
    qs = querykey
    qs = [_qs for _qs in qs if len(_qs) >= 2]
    tfidf_vectorizer = TfidfVectorizer(analyzer='word', ngram_range=(1, 1))
    # print(querykey)
    tfidf_vectorizer.fit(querykey)
    
    arr = tfidf_vectorizer.transform(flat_list).toarray()
    qrytfidf = [1] *len(qs)
    if sum(arr[np.argmax(arr.sum(axis=1))]) != 0:
        return cos_sim(arr[np.argmax(arr.sum(axis=1))], qrytfidf)
    else:
        return 0

def cos_sim(A, B):
    return dot(A, B)/(norm(A)*norm(B))
