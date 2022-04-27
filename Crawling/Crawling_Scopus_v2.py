import re, math, datetime, time
from pymongo import MongoClient
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

client = MongoClient('203.255.92.141:27017')

scopus_db = client['SCOPUS'] 
scopus_col = scopus_db['Author']

change_list_count = scopus_col.count_documents({ '$and': [{ '_id': {'$ne': ""} }, { 'check':0 }]}) #check가 0인 문서 수
T = math.ceil(change_list_count/200)
print(f'처리할 Author: {change_list_count}, 반복 수: {T}')

etc_path = "/home/search/apps/product/etc"
url = 'https://www.scopus.com/search/form.uri?display=advanced'
download_path = "/home/search/apps/sh"
options = webdriver.ChromeOptions()
options_box = ['--disable-dev-shm-usage', '--headless', '--disable-gpu', '--no-sandbox', 'user-agent=Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 640 XL LTE) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10166']

for i in range(len(options_box)):
    options.add_argument(options_box[i])

options.add_experimental_option("prefs", {
    "download.default_directory": download_path,
    "download.prompt_for_download": False,
    'profile.default_content_setting_values.automatic_downloads': 1,
    })

driver = webdriver.Chrome(r"/home/search/apps/product/etc/chromedriver", chrome_options=options)
driver.get(url)

for col_case in range(1, T+1):

    list_count = scopus_col.count_documents({'check':1}) #check가 1인 문서 수
    try:
        start = time.time()
        change_list = scopus_col.find({ '$and': [{ '_id': {'$ne': ""} }, { 'check':0 }]}, '_id') #check = 0인 col
        au_id = '' #SCOPUS AU-ID 검색
        au_id_list = [] #SCOPUS ID 목록

        for x in change_list[0:200]:
            au_id += ('AU-ID('+ x['_id'] + ')')
            au_id_list.append(x['_id'])

        element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "searchfield")))
        element.clear()
        element.send_keys(au_id)

        #scopus id 검색
        element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "advSearch")))
        element.click()

        #edit에서 전체 저자 목록
        driver.execute_script("processLinkClick('editSearchButton')")

        element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "searchfield")))
        author_fname = re.findall('"(.+?)"', element.text)

        for i in range(0, len(au_id_list)):
            id_query = {'$and': [{ '_id':au_id_list[i] }, { 'check':0 }]}
            change_name = { '$set': { 'name': author_fname[i] } }
            change_label = { '$set': { 'check': 1 } }
            
            scopus_col.update_many(id_query, change_name)
            scopus_col.update_many(id_query, change_label)

        end = time.time()
        print(f'현재 처리된 Author: {list_count}, 처리 시간: {end - start:.5f} sec')
        time.sleep(2)

    except Exception as e:
        print(f'현재 처리된 Author: {list_count}, 에러 발생 시간: {datetime.datetime.today()}')
        print(e)

driver.quit()
print('완료')

