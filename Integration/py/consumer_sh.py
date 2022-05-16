import threading, logging, re, time, datetime, json, multiprocessing
from analyzer.analyzer import analyzerProject
from analyzer.analyzer import analyzerPaper
from pymongo.errors import BulkWriteError
from multiprocessing import Process
from pymongo import MongoClient
from kafka import KafkaConsumer
from json import loads


""" #1. 컨슈머 객체 생성 (Thread) """
class Consumer(threading.Thread):
    #MongoDB URI
    client =  MongoClient('localhost:27017', connect=False)
    db = None

    """
        @__init__   : 객체 생성자
			@site       : 수집 site 명을 입력 (site 별로 Consumer 객체 생성)
    """
    def __init__(self, site):
        threading.Thread.__init__(self)

        self.cores = multiprocessing.cpu_count()
        if self.cores > 3 :
            self.cores -= 1

        self.site                       = site
        self.collections                = ['AuthorRelation', 'QueryKeyword', 'AuthorPapers', 'Author', 'Rawdata', 'ExpertFactor', 'ExpertFactorTable']
        self.dbs                        = {}
        self.Author_info_Dic            = {}
        db                              = self.client[self.site]

        db2                             = self.client['PUBLIC']
        self.dbs['public_QueryKeyword'] = db2.QueryKeyword
        self.dbs['DBPIA_CRAWLER']       = db2.DBPIA_CRAWLER

        for col in self.collections :
            self.dbs[col] = db[col]

        #Kafka URI -> Porting 시 수정
        bootstrap_servers = ["203.255.92.141:9092"]
        self.consumer = KafkaConsumer(
            self.site,
            bootstrap_servers=bootstrap_servers,
            group_id=self.site,
            enable_auto_commit=True,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x),
            max_poll_records=1,
            # session_timeout_ms=250000,
            # request_timeout_ms=500000,
            # max_poll_interval_ms=250000,
            # max_poll_interval_ms =int(300000),
            )
    """
        @logging_time   : 함수 시간 측정 용, 현재 사용 x
            @original_fn    : 측정하고자 하는 함수 입력
    """
    def logging_time(original_fn):
        def wrapper_fn(*args, **kwargs):
            start_time = time.time()
            result = original_fn(*args, **kwargs)
            end_time = time.time()
            print("WorkingTime[{}]: {} sec".format(original_fn.__name__, end_time-start_time))
            return result
        return wrapper_fn

    """ #6-1. AuthorPapers 삽입 ( > th)"""
    """
        @checkAndInsertAuthorPapers : 주기적으로 메모리에 있는 정보를 DB에 저장 (Author 정보)
            @keyId                      : 검색 ID
            @th                         : 임계 값 (이 수치보다 많은 사용자가 저장되게되면 바로 DB에 저장, default 1000)
    """
    #@logging_time
    def checkAndInsertAuthorPapers(self, keyId, th=1000) :
        if len(self.Author_info_Dic.get(keyId, {})) > th : #th 보다 많으면 update
            print(self.site, " ----------------------------------- insert AuthorPapers")
            for i in self.Author_info_Dic[keyId]:
                self.dbs['AuthorPapers'].update({"keyId" : keyId, "A_ID" : i}, {'$push' : {"papers" : {"$each" : self.Author_info_Dic[keyId][i]}}}, upsert=True, multi=False)
            self.Author_info_Dic[keyId] = {}

    """ #6. progress  """
    """
        @processing_rate    : 진행률, 진행 상태 저장 함수, 진행이 완료되면 분석기 실행
            @data               : producer로 부터 받은 message
            @keyId              : 검색 ID
    """
    #@logging_time
    def processing_rate(self, data, keyId):


        raw_progress = data["progress"] * 100  # raw data 진행률 받아오기

        """ #6-1. AuthorPapers 삽입 ( > th)"""
        self.checkAndInsertAuthorPapers(keyId)

        """ #6-2. progress update, 이전 정보와 1퍼센트 차이 / 이전 진행률보다 낮은 경우 (KCI) """
        if keyId not in self.pre_progress:
            self.pre_progress[keyId] = 0
        if (self.pre_progress[keyId] + 1 <= raw_progress ) and (raw_progress != 100) or self.pre_progress[keyId] > raw_progress:
            print(self.site,"=>", keyId, str(raw_progress))
            self.dbs['QueryKeyword'].update({"_id": keyId}, {'$set': {"progress": str(raw_progress)}})
            self.pre_progress[keyId] = raw_progress
        elif (raw_progress == 100):

            """ #6-3. progress 100 처리, 수집 완료 => 분석기 실행 필요 """
            print(self.site,"=>", keyId,str(raw_progress))
            dt = datetime.datetime.now()
            self.consumer.commit()
            count = self.dbs['Rawdata'].count({"keyId": keyId})

            """ #6-1. AuthorPapers 삽입 ( > th)"""
            self.checkAndInsertAuthorPapers(keyId, 0)
            isPartial = False
            if 'isPartial' in data :
                isPartial = data['isPartial']

            """ #7. 분석기 실행 (Process caller)"""
            if self.dbs['QueryKeyword'].find_one({"_id":keyId})['state'] == 0 :
                self.dbs['QueryKeyword'].update({"_id": keyId}, {'$set': {"progress": 0, "state": 1, "data": count, "crawl_time": dt.strftime("%Y-%m-%d %H:%M:%S"), "isPartial" : isPartial}})
                Process(target= self.runAnalyzer, args=(keyId,)).start()

            if keyId in  self.Author_info_Dic :
                del self.Author_info_Dic[keyId]
            del self.pre_progress[keyId]

    """ #7. 분석기 실행 (Process runner)"""
    """
        @runAnalyzer    : 분석기 실행 함수
            @keyId          : 검색 ID
    """
    #@logging_time
    def runAnalyzer(self, keyId) :
        """ #7-1. 분석기 실행 전 사전 작업,  query 분석 / node 작업 분배 / normalization back data 삽입 """

        """ #7-1. 분석기 실행 전 사전 작업, normalization back data 삽입 """
        authorSize = self.dbs['AuthorPapers'].count({"keyId":keyId})
        qTemp = {'Quality' : -1, 'Productivity' : -1, 'Contrib' : -1}
        if(self.site == 'NTIS') :
            qTemp['Coop'] = -1
        qTemp['_id'] = keyId

        self.dbs['ExpertFactorTable'].insert(qTemp)

        """ #7-1. 분석기 실행 전 사전 작업, query 분석 """
        qry = self.dbs['public_QueryKeyword'].find_one({"_id":keyId})['query_keyword']
        qryResult = re.sub('[|!]',"", qry)


        """ #7-1. 분석기 실행 전 사전 작업, node 작업 분배 (데이터를 core 갯수로 나눴을때 > th 많다면 작업을 분배) """
        th = 100 # each core handle 100 or more data

        sizeDict = {}
        perData = int(authorSize / self.cores)
        if perData > th :
            last = 0
            for i in range(self.cores-1) :
                sizeDict[last] = last+perData
                last += perData
            sizeDict[last] = authorSize
        else :
            sizeDict[0] = authorSize

        """ #7-2. 분석기 실행 multiprocessing (데이터가 적으면 1개만 수행)"""
        processList = []
        for key in sizeDict :
            acl = None
            if self.site == 'NTIS':
                acl = analyzerProject(keyId, self.site, qryResult, key, sizeDict[key], authorSize)

            else :
                acl = analyzerPaper(keyId, self.site, qryResult, key, sizeDict[key], authorSize)

            p = Process(target= acl.run)
            processList.append(p)
            p.start()
        for p in processList :
            p.join()



        """ #8-1. normalization """
        maxFactors = ['Quality' , 'Productivity' , 'Contrib' ]
        if self.site != 'NTIS' :
            maxFactors.append('Coop')

        finMaxEf = self.dbs['ExpertFactorTable'].find_one({"_id" : keyId})
        setOpts = []

        for k in maxFactors :
            tempStr = { "$set" : { k  : { "$multiply" : [1/finMaxEf[k], "$"+k] }}}
            setOpts.append(tempStr)
        self.dbs['ExpertFactor'].update_many({"keyId":keyId}, setOpts)


        print("normaliztioned")
        """ #9. 분석 완료 """
        dt = datetime.datetime.now()
        self.dbs['QueryKeyword'].update({"_id":keyId},{'$set':{"progress":100, "state":2, "experts" : authorSize, "a_time" : dt.strftime("%Y-%m-%d %H:%M:%S")}})
        print('analyzer end')


    """ Note #1. 저자 ID 생성 helper 1 """
    """
        @getAndInsert   : 저자 ID 중복확인 함수
            @ID             : 저자 ID
            @name           : 저자 명
            @Inst           : 저자 소속
    """
    #@logging_time
    def getAndInsert(self, ID, name, Inst):
        search_Id = self.dbs['Author'].find_one({"_id":ID})
        if search_Id is None :
            self.dbs['Author'].insert_one({'_id': ID, 'name': name, 'inst': Inst})


    """ Note #2. 저자 ID 생성 helper 2 """
    """
        @getAndInsert   : 저자 ID 중복확인 함수
            @ID             : 저자 ID
            @name           : 저자 명
            @issuing        : 저자 소속 (혹은 저널명)
            @tempAids       : 저자 ID 목록 (DBPIA 같은 사이트는 ID목록으로 조회 가능)
            @idx            : 저자 ID 목록 중 어느 저자인지 확인하기 위한 Index
    """
    #@logging_time
    def searchA_ID(self, name, issuing, tempAids, idx):
        if len(tempAids) == 0:       # scienceon
            search_Id = self.dbs['Author'].find_one({"name": name, "inst": issuing})
        else:       # dbpia
            search_Id = self.dbs['Author'].find_one({"_id" : tempAids[idx]})
        return  search_Id['_id'] if search_Id is not None else 0

    """ Note #3. 저자 ID 생성 helper 3 """
    """
        @getAndInsert   : NTIS용 책임연구자 ID 확인 및 업데이트 용
            @name           : 책임 연구자 명
            @mng_ID         : 책임 연구자 ID
            @tempAids       : 저자 ID 목록 (DBPIA 같은 사이트는 ID목록으로 조회 가능)
            @idx            : 저자 ID 목록 중 어느 저자인지 확인하기 위한 Index
    """
    #@logging_time
    def checkAndUpdateInst(self, name, mng_ID, inst):
        mng_rsc = self.dbs['Author'].find_one({"_id":mng_ID})
        if mng_rsc is not None and mng_rsc["inst"] != inst:
            self.dbs['Author'].update({"_id":mng_ID},{'$set':{"inst":inst}})
        return 0

    """ Note #4 저자 ID 생성 helper 4 """
    """
        @insertID   : 저자 ID 입력 함수
            @name           : 저자 명
            @inst           : 소속 명
            @tempAids       : 저자 ID 목록
            @idx            : 저자 ID 목록 중 어느 저자인지 확인하기 위한 Index
            @site           : data의 site 명 : SCOPUS FULL NAME check 필드 삽입을 위해 추가
    """
    #@logging_time
    def insertID(self, name, inst, tempAids, idx, site):
        tempId = ""
        insertData = {}

        if len(tempAids) == 0 :
            tempId = "s"+str(self.cnt)  # 중복이 없으면 아이디 부여
            self.cnt +=1
        else :
            tempId = tempAids[idx]
            if self.site == 'DBPIA':
                insertData['hasInst'] = False
                self.dbs['DBPIA_CRAWLER'].find_one_and_update({"_id": 4865}, {"$inc": {"total": 1}})

        insertData['name'] = name
        insertData['inst'] = inst

        while self.dbs['Author'].find_one({"_id" : tempId}) is not None :
            tempId = "s"+str(self.cnt)  # 중복이 없으면 아이디 부여
            self.cnt +=1

        insertData['_id'] = tempId
        if site == 'SCOPUS':
            insertData['check'] = 0
        self.dbs['Author'].insert_one(insertData)
        return tempId


    """ Note #5 저자 ID 생성 helper 5 """
    """
        @search_info    : 저자 ID 검색
            @id             : 저자 ID
    """
    #@logging_time
    def search_info(self, id):
        temp = self.dbs['Author'].find_one({"_id": id})
        return temp

    """ #5. 저장 ID 생성 """
    """
        @getAIds    : 수집 된 정보 (ID, 이름, 소속)를 가지고 저자 정보 테이블에 요청(및 생성)하여 생성된 ID 목록을 반환
            @data       : kafka 로 입력된 message
    """
    #@logging_time
    def getAIds(self, data):
        A_id = []
        tempAids = []
        if self.site == 'NTIS' :
            mng_Name = data["mng"]
            mng_ID   = ""
            Inst     = ""
            if data['perfAgent'] is not None and data['perfAgent']['@code'] == '03':
                Inst = data["ldAgency"].replace(' <span class=\search_word\>','').replace('</span>','')
                # 책임연구자 db저장
                if data["mngId"] == 'null' or data["mngId"] == None:    # mngId가 없는경우
                    tempAid = self.searchA_ID(mng_Name, Inst, tempAids, -1)

                    if tempAid == 0:
                        mng_ID = 's'+ str(self.cnt)
                        self.cnt +=1
                    else:   # mngId가 있는경우
                        mng_ID = tempAid
                        self.checkAndUpdateInst(mng_Name, mng_ID, Inst)        # 책임 연구자, 참여 연구자 모두 해당 될 경우 책임연구자 소속으로 소속 변경
                else :
                    idx = data["mngId"].find('.')
                    mng_ID = re.sub('"', '', data["mngId"])[idx+1:]
                    self.checkAndUpdateInst(mng_Name, mng_ID, Inst)

                self.getAndInsert(mng_ID, mng_Name, Inst)                      # Author에 저장되어 있는지 확인. 없으면 0, 있으면 해당

                data['mng']   = mng_Name
                data['mngId']= mng_ID
                A_id.append(mng_ID)

                # 참여연구자 db저장
                if data["rsc"] != None and data["rscId"] != None:
                    rscs   = re.sub('"', "", data["rsc"]).split(";")
                    rscIds = re.sub('"', "", data["rscId"]).split(";")
                    rsc_names = []
                    rsc_ids = []
                    for idx in range(len(rscIds)):
                        if rscIds[idx] != "없음" and rscIds[idx] != "null":   # id가 존재하는 경우
                            rscIds[idx] = re.sub('"','',rscIds[idx])[rscIds[idx].find('.')+1:]
                            temp = self.search_info(rscIds[idx])
                            if temp is not None :
                                self.getAndInsert(rscIds[idx], temp['name'], temp['inst'])
                                rsc_names.append(temp['name'])
                                rsc_ids.append(rscIds[idx])
                                A_id.append(rscIds[idx])
                            else: # 이름 체크
                                if idx<len(rscs):
                                    rsc_name = rscs[idx].replace('<span class=\search_word\>','').replace('</span>','')
                                    if rsc_name != "없음" and rsc_name != "null" and "..." not in rsc_name:
                                        self.getAndInsert(rscIds[idx], rsc_name, Inst)
                                        rsc_names.append(rsc_name)
                                        rsc_ids.append(rscIds[idx])
                                        A_id.append(rscIds[idx])
                    data["rsc"] = ';'.join(rsc_names)
                    data["rscId"] = ';'.join(rsc_ids)

                self.dbs['Rawdata'].insert_one(data)

# WOS r_id, o_id 삽입 및 업데이트
        elif self.site == 'WOS': # WOS
            ids = []
            if data["author"] is None :
                return []

            names = data["author"].split(";")
            rids = data["R_id"].split(";")
            oids = data["O_id"].split(";")
            author_inst = data["author_inst"].split(";")
            issuing = data["issue_inst"]
            tempAids = []
            processedNames = ""
            processedInsts = ""

            for i in range(len(names)):
                insertData = {}
                names[i] = names[i].strip()
                author_inst[i] = author_inst[i].strip()

                insertData['name'] = names[i]
                insertData['inst'] = author_inst[i]
                insertData['R_id'] = rids[i]
                insertData['O_id'] = oids[i]
 

                if 'affiliationId' in author_inst[i]:
                    Inst = author_inst[i].replace('affiliationId type=','').replace('&gt','')
                processedNames += names[i]+";"
                processedInsts += author_inst[i]+";"


                tempId = self.searchA_ID(names[i], author_inst[i], tempAids, i)
                if tempId == 0:  # 중복 ID가 없으면
                    if len(rids[i]) != 0:
                        tempId = rids[i]
                    elif len(oids[i]) != 0:   #elif 'oid' in data:
                        tempId = oids[i]
                    else:
                        tempId = self.insertID(names[i], author_inst[i], tempAids, i, self.site)
                    insertData['_id'] = tempId
                    self.dbs['Author'].insert_one(insertData)
                elif tempId == rids[i]:
                    if len(oids[i]) !=0:
                        self.dbs['Author'].update({"_id" : tempId},{'$set':{"O_id" : oids[i]}})
                elif tempId == oids[i]:
                    if len(rids[i]) != 0:
                        self.dbs['Author'].delete_one({"_id" : tempId})
                        tempId = rids[i]
                        insertData['_id'] = tempId
                        self.dbs['Author'].insert_one(insertData)

                A_id.append(tempId)

            if len(processedNames) != 0 :
                data['author'] = processedNames
            if len(processedInsts) != 0 :
                data['author_inst'] = processedInsts

            data['author_id'] = ';'.join(A_id)
            self.dbs['Rawdata'].insert_one(data)

        else :   # SCIENCEON, DBPIA
            ids = []
            if data["author"] is None :
                return []

            names = data["author"].split(";")
            author_inst = data["author_inst"]
            issuing = data["issue_inst"]
            tempAids = []
            processedNames = ""
            processedInsts = ""

            if 'author_id' in data :
                ids = data["author_id"].split(";")
                if len(ids) != len(names) :
                    return []
                tempAids = ids

            idx = 0
            if not author_inst: # 저자의 소속이 없을때(SCIENCEON), DBPIA
                for name in names:
                    processedNames += name+";"
                    tempId     = self.searchA_ID(name, "", tempAids, idx)
                    if tempId == 0:  # 중복 ID가 없으면
                        tempId = self.insertID(name, "", tempAids, idx, self.site)
                    idx += 1
                    A_id.append(tempId)
            else:
                if 'KISTI' in data["author_inst"]:
                    length = len(author_inst.split(';'))
                    author_inst = [author_inst.split(";")[0].replace('&lt','') for i in range(length)]
                else:
                    author_inst = author_inst.split(";")

                for Name,inst in zip(names, author_inst):  # 이름, 소속 둘다 있으면
                    Name = Name.strip()
                    inst = inst.strip()

                    if 'affiliationId' in inst:
                        Inst = inst.replace('affiliationId type=','').replace('&gt','')
                    processedNames += Name+";"
                    processedInsts += inst+";"
                    tempId = self.searchA_ID(Name, inst, tempAids, idx)


                    if tempId == 0:  # 중복비교idx
                        tempId = self.insertID(Name, inst, tempAids, idx, self.site)

                    idx += 1
                    A_id.append(tempId)
            if len(processedNames) != 0 :
                data['author'] = processedNames
            if len(processedInsts) != 0 :
                data['author_inst'] = processedInsts

            data['author_id'] = ';'.join(A_id)
            if self.site != 'NTIS' :
                data['mngId'] = A_id[-1] 
            self.dbs['Rawdata'].insert_one(data)
        return A_id

    """
        @del_data    : 테스트 용
    """
    #@logging_time
    def del_data(self):
        for msg in self.consumer:             # scienceon Author collection
            if self.site == 'SCIENCEON':
                data = json.loads(msg.value)
            else:
                data = msg.value

    """ #4. 정상 msg 처리 """
    """
        @processingMsg  : 입력된 message 처리 (저자 ID 생성 -> 진행률 계산)
            @msg            : 입력된 message
    """
    #@logging_time
    def processingMsg(self, msg) :


        key_id = msg['keyId']
        """ #4-1. 저자 ID 생성 (없으면) """
        a_id = self.getAIds(msg)
	
        """ #4-2. 저자 ID 임시 저장 (AuthorPapers 삽입 위함)"""
        if len(a_id) != 0:
            object_id = msg['_id']
            if key_id not in self.Author_info_Dic :
                self.Author_info_Dic[key_id] = {}
            adic = self.Author_info_Dic[key_id]
            for aid in a_id:
                if aid in adic:
                    adic[aid].append(object_id)
                else:
                    adic[aid] = [object_id]

        """ #4-3. progress 처리 """
        self.processing_rate(msg, key_id)

    """ #2. 컨슈머 객체 실행 """
    """
        @run  : Consumer Message 처리 로직 (consumer가 살아있는한 무한으로 실행 되는 곳)
    """
    #@logging_time
    def run(self):
        logging.warning(self.site+"_Consumer on")
        self.cnt = self.dbs['Author'].count() + 1

        self.pre_progress = {}
        tempRaw = {}
        time.sleep(1)
        logging.warning(self.site+" trying to consume messages")
        tempCnt = 0
        """ #3. msg 처리기 """
        for msg in self.consumer:
            try:
                data = msg.value
                if not data:
                    pass
                else:
                    key_id = data['keyId']
                    """ #3-1. fail 처리 """
                    if 'API LIMIT' in data :
                        dt = datetime.datetime.now()
                        # 분석처리를 수행하는 코드 실행, isPartial =넘겨줌`>`
                        if len(tempRaw) > 0:
                            self.processingMsg(tempRaw)
                            self.processing_rate(data, key_id)
                        else :
                            self.dbs['QueryKeyword'].update({"_id": key_id}, {'$set': {"progress": 100, "state": -3, "data": 0, "crawl_time": dt.strftime("%Y-%m-%d %H:%M:%S")}})


                    elif 'fail' in data :
                        dt = datetime.datetime.now()
                        print("producer die")
                        self.dbs['QueryKeyword'].update({"_id": key_id}, {'$set': {"progress": 100, "state": -1, "data": 0, "crawl_time": dt.strftime("%Y-%m-%d %H:%M:%S")}})
                        if key_id in self.pre_progress:
                            del self.pre_progress[key_id]
                            del self.Author_info_Dic[key_id]

                    else :
                        """ #3-2. 정상 msg 처리 """
                        """ #3-3. 정상 msg 중 data가 없는 msg """
                        if data['progress'] == 1 and 'id' not in data:
                            if len(tempRaw) > 0:
                                self.processingMsg(tempRaw)
                            self.processing_rate(data, key_id)
                            print("Num. Dup ", tempCnt)
                            tempRaw = {}
                        else:

                            """ #3-4. 정상 msg 중 data가 있는 msg """
                            r = self.dbs['Rawdata'].find_one({'$and': [{'id': data['id']}, {'keyId': key_id}]})

                            """ #3-5. 데이터 중복 체크 """
                            if r is not None:
                                tempCnt += 1
                                """ #3-6. 마지막 msg가 중복 인 경우 """
                                if data['progress'] == 1.0 :
                                    if len(tempRaw)  == 0:
                                        self.processing_rate(data, key_id)
                                    else :
                                        tempRaw['progress'] = 1
                                        self.processingMsg(tempRaw)
                                        print("Num. Dup ", tempCnt)
                                    tempRaw = {}
                                continue
                            else:
                                """ #3-7. msg 처리 (producer의 tempRaw와 동일하게 수행)"""
                                if len(tempRaw) > 0:
                                   self.processingMsg(tempRaw)
                                tempRaw = data.copy()
                                if data['progress'] == 1 :
                                    tempRaw['progress'] = 1
                                    self.processingMsg(tempRaw)
                                    print("Num. Dup ", tempCnt)
                                    tempRaw = {}
            except Exception as e:
                """ #3-8. consumer error 처리 """
                print(tempRaw)
                print(data)
                print(e)
                print("consumer die")
                dt = datetime.datetime.now()
                self.dbs['QueryKeyword'].update({"_id": key_id}, {'$set': {"progress": 100, "state": -2, "crawl_time": dt.strftime("%Y-%m-%d %H:%M:%S")}})
                tempRaw = {}
                if key_id in self.pre_progress:
                    del self.pre_progress[key_id]
                if key_id in self.Author_info_Dic:
                    del self.Author_info_Dic[key_id]

        logging.warning(self.site+"_Consumer END")
        self.consumer.close()
