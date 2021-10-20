from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler
from watchdog.events import FileSystemEventHandler
from multiprocessing.managers import BaseManager
from collections import OrderedDict
from multiprocessing import Process, Value, Array
import time
import csv
import pprint
import re
import sys, os, re, math, logging, time, json, xlrd, retry, pprint
from json import dumps
from kafka import KafkaProducer

csv.field_size_limit(sys.maxsize)

class Parser :
    '''
    @ Method Name     : __init__
    @ Method explain  : 파싱에 필요한 파라미터
    '''
    def __init__(self ,total_data, keyId, path, isPartial, is_not_active, chrome_failed, numProcess, _ip):
        self.total_data    = total_data
        self.keyId         = keyId
        self.path          = path
        self.cnt           = 0
        self.producer      = KafkaProducer(bootstrap_servers= _ip + ':9092', value_serializer=lambda x: json.dumps(x).encode('utf-8'))
        self.isPartial     = isPartial
        self.temp          = {}
        self.paper_cnt     = 0
        self.error_count   = 0
        self.inst_error    = 0
        self.is_not_active = is_not_active
        self.chrome_failed = chrome_failed
        self.numProcess    = numProcess

    '''
    @ Method Name     : SCOPUS_Parsing
    @ Method explain  : scopus 파싱 데이터 전처리 및 정제
    '''
    def SCOPUS_Parsing(self, filename, isLast, path):
        print("scopus parsing")
        PaperRawSchema = ['id', 'title', 'author','journal', 'issue_inst', 'issue_year', 'issue_lang',
                          'start_page', 'end_page', 'paper_keyword', 'abstract', 'author_inst', 'author_id', 'citation','link']

        column_name = ['EID', 'Title', 'Authors', 'Publisher', 'Affiliations', 'Year', 'Language of Original Document',
        'Page start',	'Page end', 'Author Keywords', 'Abstract', 'Authors with affiliations', 'Author(s) ID', 'Cited by','Link']
        scopus_index = [33, 2, 0, 22, 14, 3, 27, 8, 9, 17, 16, 15, 1, 11, 13] #[36, 2, 0, 25, 14, 3, 30, 8, 9, 17, 16, 15, 1, 11, 13]

        tempRaw = {}
        # csv_path = (self.path + filename)
        csv_path = (path + filename)
        print(csv_path, "  :    엑셀파일 경로 및 이름 출력")
        print("엑셀파일 불러오기")
        # cnt = 0
        global Column
        with open(csv_path, "r", encoding="UTF-8") as f:
            # raw_data = {'english_title'}
            csv_data = csv.reader(f)
            for column in csv_data:
                Column = column
                break
            Column[0] = 'Authors'

            #Funding Text처리
            num = 1
            for i in range(len(Column)):
                if 'Funding Text ' in Column[i] :
                    scopus_index.insert(i, i)
                    column_name.insert(i, 'Funding Text %d' % num)
                    PaperRawSchema.insert(i, 'funding_text_%d' % num)
                    num += 1
                    for j in range(len(scopus_index)) :
                        if i < scopus_index[j] :
                            scopus_index[j] += 1

            column_idx = [Column.index(i) for i in column_name]                 # [12, 2, 0, 25, 14, 3, 30, 8, 9, 17, 16, 15, 1, 11]
            #print(PaperRawSchema, column_name, scopus_index)

            try:
                # next(csv_data)  # 2행부터 읽는다.

                raw_data = {}
                for row in csv_data:
                    try :
                        # row = csv_data[i]
                        # print("row:", row)
                        self.paper_cnt     += 1
                        raw_data = {'english_title' : '', 'english_abstract' : '' , 'isPartial' : 'False'}

                        try:
                            if 'No author id available' in row[scopus_index[PaperRawSchema.index('id')]] or 'No author name available' in row[scopus_index[PaperRawSchema.index('author')]] :
                                continue
                        except:
                            if 'No author id available' in row[33] or 'No author name available' in row[scopus_index[PaperRawSchema.index('author')]] :
                                continue

                        for i in range(len(PaperRawSchema)):
                            # raw_data[PaperRawSchema[i]] = row[scopus_index[i]]
                            raw_data[PaperRawSchema[i]] = row[column_idx[i]]

                        if not raw_data['title'][-1].isalnum():
                            raw_data['title']    = raw_data['title'][:-1]

                        if raw_data['citation'] == '' or raw_data['citation'] is None:
                            raw_data['citation'] = 0

                        if not raw_data['issue_inst']:
                            raw_data['issue_inst'] = ''

                        if not raw_data['start_page']:
                            raw_data['start_page'] = 0
                        else:
                            raw_data['start_page']   = re.sub('\D', '', raw_data['start_page'])

                        if not raw_data['end_page']:
                            raw_data['end_page'] = 0
                        else:
                            raw_data['end_page']     = re.sub('\D', '', raw_data['end_page'])

                        if not raw_data['paper_keyword']:
                            raw_data['paper_keyword'] = ''

                        if 'funding_text_' in raw_data:
                            for n in range(1, num):
                                if not 'funding_text_%d' % num:
                                    raw_data['funding_text_%d' % num] = ''

                        raw_data['id']           = raw_data['id'].replace('2-s2.0-', '')
                        raw_data['author_id']    = raw_data['author_id'][0:len(raw_data['author_id'])-1]
                        raw_data['author']       = raw_data['author'].replace(',', ';')
                        author_info              = raw_data['author_inst'].replace(',', '').replace(';', ' ;').split(' ')
                        author_name              = raw_data['author'].replace(';', '').split(' ')

                        author_inst = []
                        for i in author_info:
                            if i not in author_name:
                                author_inst.append(i)
                        split_author_inst = ' '.join(author_inst).split(';')

                        for i in range(len(split_author_inst)):
                            if split_author_inst[i] == ' ':
                                # print(i, split_author_inst[i])
                                split_author_inst[i] = split_author_inst[i - 1]

                            elif split_author_inst[i] == '':
                                split_author_inst[i] = split_author_inst[i - 1][:-1]

                            else:
                                pass

                        if not raw_data['author_inst']:
                            raw_data['author_inst'] = ''
                        else:
                            raw_data['author_inst'] = '; '.join(split_author_inst).replace(' ;', ';')
                        raw_data['keyId'] = self.keyId

                        raw_data['isPartial'] = self.isPartial.value
                        td = self.total_data.value
                        self.cnt += 1
                        pro = self.cnt / td
                        if pro >= 1.0 :
                            # if isLast and i == (size-1) :
                            #     pro = 1.0
                            # else :
                            pro = 0.99
                        raw_data['progress'] = pro
                        #self.producer.send(self.site, value=raw_data)
                        if len(tempRaw) != 0 :
                            self.producer.send('SCOPUS', value=tempRaw)

                        tempRaw = raw_data.copy()
                    except Exception as e :
                        print(e)
                        print(row)
                        pprint.pprint(raw_data)
                        self.error_count   += 1
                        print("====parsing ERROR====")

            except:
                print("===== empty file =====")
                self.isPartial.value = 1
        # pprint.pprint(tempRaw)
        print("====parsing SUCCESS====")
        print("")
        print("producer 보낸 논문수 : ", self.paper_cnt,'|| 총 논문수 : ', self.total_data.value)
        print('Exception 데이터 수 : ', self.error_count)
        print("")

        # print(self.producer)
        if len(tempRaw) != 0 :
            # if isLast == True :
            #     tempRaw['progress'] = 1.0
            self.producer.send('SCOPUS', value=tempRaw)
            # pprint.pprint("producer send end")
            # pprint.pprint(tempRaw)
        # elif isLast == True:
        #     self.flush()

    '''
    @ Method Name     : flush
    @ Method explain  : kafka flush 실행, 진생 상황(progress, 크롬 상태, isPartial 체크)
    '''
    def flush(self) :
        numMessges = 0
        try :
            temp = {'progress' : 1, 'keyId' : self.keyId, 'ok' : 1, 'is_not_active' : self.is_not_active.value}
            if self.is_not_active.value == 1:
                temp['API LIMIT'] = -3
            if self.chrome_failed.value == self.numProcess:
                temp['fail'] = 1
            elif self.chrome_failed.value > 0:
                self.isPartial.value = 1

            temp['isPartial'] = self.isPartial.value
            print(temp)
            self.producer.send('SCOPUS', value = temp)
            numMessges = self.producer.flush()

        except Exception as e :
            print(e)

        # print(numMessges)

    '''
    @ Method Name     : on_send_success
    @ Method explain  : kafka 데이터 보내기 성공 확인 프린트
    '''
    def on_send_success(self, record_metadata):
        print(record_metadata.topic)
        print(record_metadata.partition)
        print(record_metadata.offset)

    '''
    @ Method Name     : on_send_error
    @ Method explain  : kafka 데이터 보내기 에러 확인 프린트
    '''
    def on_send_error(self, excp):
        print(excp)
        # log.error('I am an errback', exc_info=excp)


class FileUpdateHandler(FileSystemEventHandler) :
    '''
    @ Method Name     : __init__
    @ Method explain  : 왓치독 실행에 필요한 파라미터
    '''
    def __init__(self, crawl_end, parse_end, num_data, num_parse, total_data, keyId, path, isPartial, numProcess, is_not_active, chrome_failed, _ip) :
        super(FileUpdateHandler, self).__init__()
        #print(path)
        self.crawl_end = crawl_end
        self.parse_end = parse_end
        self.num_data  = num_data
        self.num_parse = num_parse
        self.path = path
        self.isPartial = isPartial
        #print(self.path)
        self.isLast = False
        self.numProcess = numProcess
        self.is_not_active = is_not_active
        self.chrome_failed = chrome_failed
        self.parser = Parser(total_data, keyId, self.path, self.isPartial, self.is_not_active, self.chrome_failed, self.numProcess, _ip)
        # QueueManager.register('observerQ')
        # m = QueueManager(address=('localhost', 50001), authkey=b'dojin')
        # m.connect()
        # self.observerQ = m.get_queue()

    def on_any_event(self, event) :
        pass
    '''
    @ Method Name     : on_created
    @ Method explain  : 파일 생성 감지로 종료(크롬이 종료하면 파일생성됨)
    '''
    def on_created(self, event):
        fileName = event.src_path
        if "crawler_end" in fileName:
            print('Crawl_end ===> 종료 파일 생성이 감지 되었습니다.')
            self.isLast = True
            if self.num_parse.value == self.num_data.value:
                print('waiting parse')
                self.parse_end.value = 1
            else:
                self.parse_end.value = 1
        else:
            pass

    '''
    @ Method Name     : on_deleted
    @ Method explain  : 파일 삭제 감지로 최종 종료(모든 프로세스(크롬)가 종료하면 파일생성됨)
    '''
    def on_deleted(self, event):

        fileName = event.src_path
        if self.parse_end.value != 2:
            print(fileName)
            print('Crawl_end ===> 삭제가 감지 되었습니다. progress = 1 을 보냅니다.')
            self.isLast = True
            #self.parser.parsing(fileName, self.isLast)
            self.parser.flush()
            print("flush end")
            self.parse_end.value = 2


    def on_moved(self, event):
        pass

    '''
    @ Method Name     : on_modified
    @ Method explain  : 파싱 저장 경로에 파일이 생성 되면 파싱 실행
    '''
    def on_modified(self, event) :
        if not event.is_directory :
            fileName = event.src_path.split("/")[-1]
            path = event.src_path.replace(fileName,"")
            if "crdownload" not in fileName and "crawler_end" not in fileName:
                if self.num_data.value > self.num_parse.value:
                    self.num_parse.value += 1
                    self.parser.SCOPUS_Parsing(fileName, self.isLast, path)
                if self.crawl_end.value == self.numProcess:
                    print("crawl ended")
                    if self.num_data.value == self.num_parse.value :    #다운받은 파일 수와 파싱된 파일의 수가 같으면
                        print("parse ended")
                        self.isLast = True

                if self.isLast == True :
                    self.parse_end.value = 1

                print(fileName)
                print("parse", self.num_parse.value)


class FileObserver :
    '''
    @ Method Name     : __init__
    @ Method explain  : 파일 옵저버(지정 경로)에 필요한 파라미터
    '''
    def __init__(self, _path, crawl_end, parse_end, parse_data, num_data, total_data, keyId, isPartial, numProcess, is_not_active, chrome_failed, _ip):
        self.crawl_end  = crawl_end
        self.parse_end  = parse_end
        self.num_data   = num_data
        self.total_data = total_data
        # self.producer   = producer
        self.keyId      = keyId
        self.path       = _path
        self.num_parse  = parse_data
        self.isPartial  = isPartial
        self.numProcess = numProcess
        self.is_not_active = is_not_active
        self.chrome_failed = chrome_failed
        self.IP = _ip
        print(self.path)

    '''
    @ Method Name     : run
    @ Method explain  : 파일 옵저버(지정 경로) 이벤트 감지 실행
    '''
    def run(self) :
        event_handler = FileUpdateHandler(self.crawl_end, self.parse_end, self.num_data, self.num_parse, self.total_data, self.keyId, self.path, self.isPartial, self.numProcess, self.is_not_active, self.chrome_failed, self.IP)
        observer = Observer()
        observer.schedule(event_handler, self.path, recursive=True)     #recursive : 하위 디렉토리 event 감지포함
        observer.start()
        cnt = 0
        parse_error = False
        try:
            while (self.crawl_end.value != self.numProcess) or (self.parse_end.value != 2):
                time.sleep(5)
                cnt += 1
                if cnt == 3:
                    print("keyId:", self.keyId, "observer stop", self.crawl_end.value, "Parse stop : ", self.parse_end.value, "Data count : ", self.num_data.value, "Parse count : ", self.num_parse.value)
                    cnt = 0

        except KeyboardInterrupt:
            observer.stop()

        observer.stop()
        observer.join()
        print("observer end")
