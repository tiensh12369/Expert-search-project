from selenium import webdriver
import re, math, datetime, time
from pymongo import MongoClient


client = MongoClient('203.255.92.141:27017')

scopus_db = client['SCOPUS'] 
scopus_col = scopus_db['Author']

change_list_count = scopus_col.count_documents({ '$and': [{ '_id': {'$ne': ""} }, { 'check':0 }]}) #check가 0인 문서 수
T = math.ceil(change_list_count/200)
print(f'처리할 Author: {change_list_count}, 반복 수: {T}')

url = 'https://www.scopus.com/search/form.uri?display=advanced'
path = '/home/search/apps/product/etc/chromedriver'
download_path = "/home/search/apps/sh"
options = webdriver.ChromeOptions()
options_box = ['--disable-dev-shm-usage', "lang=ko_KR", '--headless', '--no-sandbox', 'User-Agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36']
for i in range(0, 4):
    options.add_argument(options_box[i])
options.add_experimental_option("prefs", {
    "download.default_directory": download_path,
    "download.prompt_for_download": False,
    'profile.default_content_setting_values.automatic_downloads': 1,
    })

for col_case in range(1, T+1):

    list_count = scopus_col.count_documents({'check':1}) #check가 1인 문서 수

    try:
        start = time.time()
        #change_list = scopus_col.find({'check':0},'_id')
        change_list = scopus_col.find({ '$and': [{ '_id': {'$ne': ""} }, { 'check':0 }]}, '_id') #check = 0인 col

        au_id = '' #SCOPUS AU-ID 검색
        au_id_list = [] #SCOPUS ID 목록

        for x in change_list[0:200]:
            au_id += ('AU-ID('+ x['_id'] + ')')
            au_id_list.append(x['_id'])

        driver = webdriver.Chrome(path, options=options)
        driver.get(url=url)

        #scopus id 검색
        time.sleep(2)
        driver.find_element_by_xpath('//*[@id="searchfield"]').send_keys(au_id)
        driver.find_element_by_xpath('//*[@id="advSearch"]').click()

        #edit에서 전체 저자 목록
        time.sleep(2)
        driver.find_element_by_xpath('//*[@id="resultsMenu"]/ul/li[1]/button').click()

        author_list = driver.find_element_by_xpath('//*[@id="searchfield"]').text

        author_fname = re.findall('"(.+?)"', author_list)
        
        for i in range(0, len(au_id_list)):
            id_query = {'$and': [{ '_id':au_id_list[i] }, { 'check':0 }]}
            change_name = { '$set': { 'name': author_fname[i] } }
            change_label = { '$set': { 'check': 1 } }
            
            scopus_col.update_many(id_query, change_name)
            scopus_col.update_many(id_query, change_label)
        
        driver.quit()
        end = time.time()
        print(f'현재 처린된 Author: {list_count}, 처리 시간: {end - start:.5f} sec')

    except Exception as e:
        driver.quit()
        print(f'현재 처린된 Author: {list_count}, 에러 발생 시간: {datetime.datetime.today()}')
        print(e)

print('완료')
