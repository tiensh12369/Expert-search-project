from time import sleep
import requests, re
from bs4 import BeautifulSoup
import random
from pymongo import MongoClient

'''
 @MongoDB 접속 URI / DB명
'''
client = MongoClient('mongodb://203.255.92.141:27017', authSource='admin')
db = client.DBPIA
pubDB = client.PUBLIC
Author = db.Author
Status = pubDB.DBPIA_CRAWLER
sl_start = 5.0

'''
대학테이블
'''
def isEnglishOrKorean(input_s):
    k_count = 0
    e_count = 0
    try:
        for c in input_s:
            if ord('가') <= ord(c) <= ord('힣'):
                k_count+=1 
            elif ord('a') <= ord(c.lower()) <= ord('z'):
                e_count+=1
        return "k" if k_count>1 else "e"
    
    except TypeError as e:
        print(input_s)
        return "e"

def check_college(univ0):
    branch_set = ['성균관대학교', '건국대학교', '한양대학교']
    univName = client['PUBLIC']['CollegeName']
    univ1 = re.sub("산학협력단|병원","",univ0)
    univ2 = re.sub("대학교","대학교 ",univ1)
    
    try:
        if univ0 == "":
            return univ0

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
                    if "건국대학교" in univ0:
                        univ[univ.index("건국대학교")] = "건국대학교 GLOCAL(글로컬)캠퍼스"
                    else :
                        univ[univ.index("성균관대학교")] = "성균관대학교"
                
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
            return univ0
        else:
            return univ_query['originalName']
        
    except SyntaxError as e:
        return univ0

'''
 @DBPIA ID를 이용해서 소속 수집 Crawling 개발 (BeautifulSoup 활용) 
'''
while True:
    soup = ""
    i = ""
    try:
        for doc in Author.find({"hasInst" : False}).batch_size(1):

            print("DBPIA Inst. Crawler :", doc['name'], doc['_id'])
            i = int( doc['_id'])
            #url 변동 되면 해당 부분 수정 필요 
            url = 'https://www.dbpia.co.kr/author/authorDetail?ancId={}'.format(i)
            conn = requests.get(url, timeout=60).text
            soup = BeautifulSoup(conn, 'html.parser')

            #Parsing 태그 정보 (변경 시 수정 필요)
            division_extract = soup.select('dd')
            name_extract = soup.select('h2')
            test_extract = name_extract[0].text.strip().split("\n")

            division = division_extract[3].text.strip()
            department = division_extract[4].text.strip()
            new_name = test_extract[0].strip()
            print(new_name)

            if '논문수' == new_name:
                Author.delete_one({"_id": doc['_id']})
                Status.find_one_and_update({"_id":4865},{"$inc":{"total":-1}})
                continue 
            
            if division == '-' and department == '-':
                inst = ''
                
            elif department=='-':
                department=''
                inst = division + department
                
            else:
                inst = division +  ' ' + department

            if len(test_extract) < 8:
                if len(test_extract) == 3:
                    papers_count = test_extract[0].strip("논문수 ")
                    used_count = test_extract[2].strip("이용수 ")
                else:
                    papers_count = test_extract[3].strip("논문수 ")
                    used_count = test_extract[5].strip("이용수 ")
                citation_count = 0
            else:
                citation_count = test_extract[7].strip("피인용수 ")

            original_inst = check_college(inst)

            Author.update_one({"_id":doc['_id']},{'$set':{"plusName" :new_name,  "inst": inst , "hasInst" : True , "papers_count" : papers_count.strip() , "used_count": used_count.strip(),'citation_count': citation_count, 'originalName': original_inst}})
            Status.find_one_and_update({"_id":4865},{"$inc":{"crawled":1}})
            requests.session().close()
            print("DBPIA Inst. Crawler :", doc['name'], ", Crawled, [inst , (paper, used, citation count)] : {} , ({}, {}, {})".format(inst.strip(), papers_count.strip(), used_count.strip(), citation_count))
            sleep(random.uniform(sl_start, sl_start + 5.0))

        Status.find_one_and_update({"_id":4865},{"$set":{"status":0}})
        print("Inst All Crawled")
        break

    except Exception as e :
        sl_start = sl_start * 1.1
        
        try :
            if '불편' in soup.select('.tit')[0].text:
                Author.update_one({"_id":doc['_id']},{'$set':{"plusName" :'',  "inst": '' , "hasInst" : True , "papers_count" : 0 , "used_count": 0,'citation_count': 0}})
                Status.find_one_and_update({"_id":4865},{"$inc":{"crawled":1}})
                print('error no id')
        except Exception as e : 
            Author.delete_one({"_id": doc['_id']})
            Status.find_one_and_update({"_id":4865},{"$inc":{"total":-1}})
            print('Another Error', e)
        print("Inst 예외", e)
        print("Sleep Time Increased by ", sl_start)
        sleep(random.uniform(sl_start, sl_start+5.0))