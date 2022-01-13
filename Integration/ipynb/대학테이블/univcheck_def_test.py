from pymongo import MongoClient
import re


'''
@ Method Name       : check_College
@ Method explain    : 저자소속을 대학DB(CollegeName)에서 검색

    @ univ0         : 저자 소속 (ex. 한국대학교000연구소산학협력단 한국대학교 빅데이터협동과정) -> 입력 파라미터
    @ univ1         : 1차(산학협력단, 병원) 전처리 (ex. 한국대학교000연구소 한국대학교 빅데이터협동과정)
    @ univ2         : 2차(대학교 ) 전처리 (ex. 한국대학교 000연구소 한국대학교 빅데이터협동과정)
    @ univ          : 3차(중복처리 및 리스트 요소로 분할) 전처리 (ex. ['한국대학교', '000연구소', '빅데이터협동과정'])
    @ univs         : 입력 쿼리 생성 (ex. {'$or': [{'inputName': '한국대학교'}, {'inputName': '000연구소'}, {'inputName': '빅데이터협동과정'}]} )
    @ univ_query    : 쿼리 결과 값 ('_id', 'originalName', 'inputName') -> 리스트 요소들을 돌면서 하나라도 검색이 가능하면, OK
'''

univ0 = "충북대 생활과학연구소"

def check_college(univ0):

    client = MongoClient('mongodb://203.255.92.141:27017', authSource='admin')
    univName = client['PUBLIC']['CollegeName']
    
    univ1 = re.sub("산학협력단|병원","",univ0)
    univ2 = re.sub("대학교","대학교 ",univ1)
    try:
        univ = univ2.replace(",", "").split()
        univ = list(set(univ))
        univs = "{'$or': ["
        for u in range(len(univ)):
            if univ[-1] == univ[u]:
                univs += "{'inputName': '" + univ[u] + "'}"
            else:
                univs += "{'inputName': '" + univ[u] + "'}, "
        univs += "]}"

        univ_query = univName.find_one(eval(univs))
        
    except SyntaxError as e:
        print(e)
        print(univ0)
        pass

    if univ_query is None:
        print("Search Fail")
        print(univ0)
    else:
        print("rawInput:[",univ0,"]","queryOutput:" ,univ_query['originalName'])
        
    return univ_query['originalName'] #univ0, univ_query

check_college(univ0)