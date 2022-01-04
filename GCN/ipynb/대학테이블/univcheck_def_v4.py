import pandas as pd
from pymongo import MongoClient
import re

'''
@ Method Name       : check_College_v2
@ Method explain    : 저자소속을 대학DB(CollegeName)에서 검색 + 국내 영문 대학명 처리(reg.match())
@ Method Fixes      : 소유격('s) 문제 처리 + 분교 임시조치
    @ univ0         : 저자 소속 (ex. 한국대학교000연구소산학협력단 한국대학교 빅데이터협동과정)
    @ univ1         : 1차(산학협력단, 병원) 전처리 (ex. 한국대학교000연구소 한국대학교 빅데이터협동과정)
    @ univ2         : 2차(대학교 ) 전처리 (ex. 한국대학교 000연구소 한국대학교 빅데이터협동과정)
    @ univ          : 3차(중복처리 및 리스트 요소로 분할) 전처리 (ex. ['한국대학교', '000연구소', '빅데이터협동과정'])
    @ univs         : 입력 쿼리 생성 (ex. {'$or': [{'inputName': '한국대학교'}, {'inputName': '000연구소'}, {'inputName': '빅데이터협동과정'}]} )
    @ univ_query    : 쿼리 결과 값 ('_id', 'originalName', 'inputName') -> 리스트 요소들을 돌면서 하나라도 검색이 가능하면, OK
    @ error_set     : 검색이 안된 소속 set
    @ answer_set    : 검색 성공한 소속 set
    @ query_set     : 검색 성공한 소속(answer_set)의 쿼리(univ_query)
'''

def isEnglishOrKorean(input_s):
    k_count = 0
    e_count = 0
    for c in input_s:
        if ord('가') <= ord(c) <= ord('힣'):
            k_count+=1
        elif ord('a') <= ord(c.lower()) <= ord('z'):
            e_count+=1
    return "k" if k_count>1 else "e"


client = MongoClient('mongodb://203.255.92.141:27017', authSource='admin')
univName = client['PUBLIC']['CollegeName']
xlsx = pd.read_excel('C:/Users/정상준/Documents/네이트온 받은 파일/NTIS 과제수행기관명 (2011~2021, 대학).xlsx')

answer_set = []
query_set = []
error_set = []
#### 추가 부분
branch_set = ['성균관대학교', '건국대학교', '한양대학교']
#### 여기까지
univ0 = "성균관대학교(자연과학캠퍼스)"

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

    #### 변동 부분    
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
    #### 여기까지
    
    univ_query = univName.find_one(eval(univs))

    if univ_query is None:
        error_set.append(univ0)
    else:
        answer_set.append(univ0)
        query_set.append(univ_query)

except SyntaxError as e:
    print(e)
    print(univ0)
    pass

for i, j in zip(answer_set, query_set):
    print("rawInput:[",i,"]","queryOutput:" ,j)