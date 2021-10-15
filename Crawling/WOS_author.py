from selenium import webdriver
from selenium.common.exceptions import ElementNotVisibleException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.alert import Alert
from multiprocessing import Process, Value, Array
from multiprocessing.managers import BaseManager
from selenium.webdriver.common.keys import Keys
from json import dumps
from time import sleep
import sys, os, re, math, logging, time, json, pprint
from bs4 import BeautifulSoup

class WOS_author:

    def __init__(self):
        self.url = "https://www.webofscience.com/wos/author/search"
        self.path = "/home/search/apps/product/etc/chromedriver"
        self.download_path = "/home/search/apps/jw"
        self.options = webdriver.ChromeOptions()
        self.options_box = ['--disable-dev-shm-usage', "lang=ko_KR", '--no-sandbox',
                            'user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36']
        for i in range(0, 4):
            self.options.add_argument(self.options_box[i])
        self.options.add_experimental_option("prefs", {
            "download.default_directory": self.download_path,
            "download.prompt_for_download": False,
            'profile.default_content_setting_values.automatic_downloads': 1,
        })
        self.driver = webdriver.Chrome(self.path, options=self.options)

        self.rawdata = {}



    def author_page(self):
        try:
            self.driver.find_element_by_xpath('/html/body/app-wos/div/div/main/div/app-input-route/app-author-record/div/div[2]/div[2]/app-citation-network/mat-card/mat-card-content/div[6]/h3')


            author_name = self.driver.find_element_by_xpath('/html/body/app-wos/div/div/main/div/app-input-route/app-author-record/div/div[2]/div[1]/app-author-record-header/mat-card/mat-card-header/div[2]/div[1]/mat-card-title/h1').text
            self.rawdata['author_name'] = author_name
            research_ID = self.driver.find_element_by_xpath('/html/body/app-wos/div/div/main/div/app-input-route/app-author-record/div/div[2]/div[1]/app-author-record-header/mat-card/mat-card-header/div[2]/div[1]/mat-card-subtitle/div[2]/a').text
            self.rawdata['research_ID'] = research_ID
    
    
            # /html/body/app-wos/div/div/main/div/app-input-route/app-author-record/div/div[2]/div[1]/app-author-record-author-details/div/mat-card/div[1]/div
    
            published_name = self.driver.find_element_by_xpath('/html/body/app-wos/div/div/main/div/app-input-route/app-author-record/div/div[2]/div[1]/app-author-record-author-details/div/mat-card/div[1]/div[2]').text
            self.rawdata['published_name'] = published_name
    
    
    
            self.organization = {}
            z = 1
            while True:
                try:
                    i = str(z)
                    a = self.driver.find_element_by_xpath('/html/body/app-wos/div/div/main/div/app-input-route/app-author-record/div/div[2]/div[1]/app-author-record-author-details/div/mat-card/div[2]/div[2]/div[' + i + ']/span[2]').text
                    b = self.driver.find_element_by_xpath('/html/body/app-wos/div/div/main/div/app-input-route/app-author-record/div/div[2]/div[1]/app-author-record-author-details/div/mat-card/div[2]/div[2]/div[' + i + ']/span[3]').text
                    self.organization[a] = b
                    z += 1
                except:
                    break
            self.rawdata['organization'] = self.organization

            award_check = self.driver.find_element_by_xpath('/html/body/app-wos/div/div/main/div/app-input-route/app-author-record/div/div[2]/div[1]/app-author-record-author-details/div/mat-card/div[3]/div[1]/span').text
            print(award_check)
            if award_check == "Awards" or award_check == "수상":

                try:
                    awards = self.driver.find_element_by_xpath('/html/body/app-wos/div/div/main/div/app-input-route/app-author-record/div/div[2]/div[1]/app-author-record-author-details/div/mat-card/div[3]/div[2]').text
                    self.rawdata['awards'] = awards
                except:
                    pass

                try:
                    other_identifiers = self.driver.find_element_by_xpath('/html/body/app-wos/div/div/main/div/app-input-route/app-author-record/div/div[2]/div[1]/app-author-record-author-details/div/mat-card/div[4]/div[2]/div[2]/a').text
                    self.rawdata['other_identifiers'] = other_identifiers
                except:
                    self.rawdata['other_identifiers'] = None
            else:
                self.rawdata['awards'] = None
                try:
                    other_identifiers = self.driver.find_element_by_xpath('/html/body/app-wos/div/div/main/div/app-input-route/app-author-record/div/div[2]/div[1]/app-author-record-author-details/div/mat-card/div[3]/div[2]/div[2]/a').text
                    self.rawdata['other_identifiers'] = other_identifiers
                except:
                    self.rawdata['other_identifiers'] = None


            H_index = self.driver.find_element_by_xpath('/html/body/app-wos/div/div/main/div/app-input-route/app-author-record/div/div[2]/div[2]/app-citation-network/mat-card/mat-card-content/div[2]/div[1]/div[1]').text
            self.rawdata['H_index'] = H_index
            total_publication = self.driver.find_element_by_xpath('/html/body/app-wos/div/div/main/div/app-input-route/app-author-record/div/div[2]/div[2]/app-citation-network/mat-card/mat-card-content/div[2]/div[2]/div[1]').text
            self.rawdata['total_publications'] = total_publication
            sum_of_times_cited = self.driver.find_element_by_xpath('/html/body/app-wos/div/div/main/div/app-input-route/app-author-record/div/div[2]/div[2]/app-citation-network/mat-card/mat-card-content/div[2]/div[3]/div[1]').text
            self.rawdata['sum_of_times_cited'] = sum_of_times_cited
            citing_articles = self.driver.find_element_by_xpath('/html/body/app-wos/div/div/main/div/app-input-route/app-author-record/div/div[2]/div[2]/app-citation-network/mat-card/mat-card-content/div[2]/div[4]/button').text
            self.rawdata['citing_articles'] = citing_articles
            verified_peer_reviews = self.driver.find_element_by_xpath('/html/body/app-wos/div/div/main/div/app-input-route/app-author-record/div/div[2]/div[2]/app-citation-network/mat-card/mat-card-content/div[4]/div[1]/div[1]').text
            self.rawdata['verified_peer_reviews'] = verified_peer_reviews
            verified_editor_records = self.driver.find_element_by_xpath('/html/body/app-wos/div/div/main/div/app-input-route/app-author-record/div/div[2]/div[2]/app-citation-network/mat-card/mat-card-content/div[4]/div[2]/div[1]').text
            self.rawdata['verified_editor_records'] = verified_editor_records
    
            author_position = {}
            author_position['First'] = self.driver.find_element_by_css_selector('#author-position-barchart > text:nth-child(3)').text
            author_position['Last'] = self.driver.find_element_by_css_selector('#author-position-barchart > text:nth-child(6)').text
            author_position['Corresponding'] = self.driver.find_element_by_css_selector('#author-position-barchart > text:nth-child(9)').text
            self.rawdata['author_position'] = author_position
    
            self.top_co_author = {}
            z = 1
            while True:
                try:
                    i = str(z)
                    a = self.driver.find_element_by_xpath('/html/body/app-wos/div/div/main/div/app-input-route/app-author-record/div/div[2]/div[2]/app-citation-network/mat-card/mat-card-content/div[6]/div[2]/div['+i+']/a').text
                    b = self.driver.find_element_by_xpath('/html/body/app-wos/div/div/main/div/app-input-route/app-author-record/div/div[2]/div[2]/app-citation-network/mat-card/mat-card-content/div[6]/div[2]/div['+i+']/span').text
                    self.top_co_author[a] = b
                    z += 1
                except:
                    break
            self.rawdata['top_co_author'] = self.top_co_author

            self.title_timescited()
    
            print(self.rawdata)
        except:
            self.none_mark_author_page()


    def none_mark_author_page(self):
        print("마크없는 저자정보 페이지")


        author_name = self.driver.find_element_by_xpath('/html/body/app-wos/div/div/main/div/app-input-route/app-author-record/div/div[2]/div[1]/app-author-record-header/mat-card/mat-card-header/div[2]/div/mat-card-title/h1').text
        self.rawdata['author_name'] = author_name
        research_ID = self.driver.find_element_by_xpath('/html/body/app-wos/div/div/main/div/app-input-route/app-author-record/div/div[2]/div[1]/app-author-record-author-details/div/mat-card/div[3]/a').text
        self.rawdata['research_ID'] = research_ID
        published_name = self.driver.find_element_by_xpath('/html/body/app-wos/div/div/main/div/app-input-route/app-author-record/div/div[2]/div[1]/app-author-record-author-details/div/mat-card/div[1]/div[2]').text
        self.rawdata['published_name'] = published_name

        self.organization = {}
        z = 1
        while True:
            try:
                i = str(z)
                a = self.driver.find_element_by_xpath('/html/body/app-wos/div/div/main/div/app-input-route/app-author-record/div/div[2]/div[1]/app-author-record-author-details/div/mat-card/div[2]/div[2]/div[' + i + ']/span[2]').text
                b = self.driver.find_element_by_xpath('/html/body/app-wos/div/div/main/div/app-input-route/app-author-record/div/div[2]/div[1]/app-author-record-author-details/div/mat-card/div[2]/div[2]/div[' + i + ']/span[3]').text
                self.organization[a] = b
                z += 1
            except:
                break
        self.rawdata['organization'] = self.organization

        self.rawdata['awards'] = None
        self.rawdata['other_identifiers'] = None

        H_index = self.driver.find_element_by_xpath('/html/body/app-wos/div/div/main/div/app-input-route/app-author-record/div/div[2]/div[2]/app-citation-network/mat-card/mat-card-content/div[2]/div[1]/div[1]').text
        self.rawdata['H_index'] = H_index
        total_publication = self.driver.find_element_by_xpath('/html/body/app-wos/div/div/main/div/app-input-route/app-author-record/div/div[2]/div[2]/app-citation-network/mat-card/mat-card-content/div[2]/div[2]/div[1]').text
        self.rawdata['total_publications'] = total_publication
        sum_of_times_cited = self.driver.find_element_by_xpath('/html/body/app-wos/div/div/main/div/app-input-route/app-author-record/div/div[2]/div[2]/app-citation-network/mat-card/mat-card-content/div[2]/div[3]/div[1]').text
        self.rawdata['sum_of_times_cited'] = sum_of_times_cited
        citing_articles = self.driver.find_element_by_xpath('/html/body/app-wos/div/div/main/div/app-input-route/app-author-record/div/div[2]/div[2]/app-citation-network/mat-card/mat-card-content/div[2]/div[4]/button').text
        self.rawdata['citing_articles'] = citing_articles
        self.rawdata['verified_peer_reviews'] = None
        self.rawdata['verified_editor_records'] = None

        author_position = {}
        author_position['First'] = self.driver.find_element_by_css_selector('#author-position-barchart > text:nth-child(3)').text
        author_position['Last'] = self.driver.find_element_by_css_selector('#author-position-barchart > text:nth-child(6)').text
        author_position['Corresponding'] = self.driver.find_element_by_css_selector('#author-position-barchart > text:nth-child(9)').text
        self.rawdata['author_position'] = author_position

        self.rawdata['top_co_author'] = None

        self.title_timescited()

        print(self.rawdata)




    def title_timescited(self):
        self.title_timescited = {}

        self.title_timescited_first()

        z = 2
        num = 1
        title_timescited_page = self.driver.find_element_by_xpath('//*[@id="mat-tab-content-0-0"]/div/div/div[1]/app-page-controls/div/form/div/span').text
        title_timescited_page_num = int(title_timescited_page)
        while True:
            try:
                i = str(z)
                a = self.driver.find_element_by_xpath('//*[@id="mat-tab-content-0-0"]/div/div/div[2]/app-publication-card['+ i +']/mat-card/app-article-metadata/div[1]/div[1]/a').text
                b = self.driver.find_element_by_xpath('//*[@id="mat-tab-content-0-0"]/div/div/div[2]/app-publication-card['+ i +']/mat-card/app-article-metadata/div[1]/div[2]/div/div').text
                self.title_timescited[a] = b
                z += 1

            except:
                try:
                    if num == title_timescited_page_num:
                        break
                    else:
                        self.driver.find_element_by_css_selector('#mat-tab-content-0-0 > div > div > div:nth-child(2) > app-page-controls > div > form > div > button:nth-child(4)').click()
                        time.sleep(10)
                        print('논문:인용횟수 다음페이지')
                        self.title_timescited_first()
                        z = 2
                        num += 1
                        continue
                except:
                    break

        self.rawdata['title:timescited'] = self.title_timescited


    def title_timescited_first(self):
        a = self.driver.find_element_by_xpath('//*[@id="snProfilesPublicationsTop"]').text
        b = self.driver.find_element_by_xpath('//*[@id="mat-tab-content-0-0"]/div/div/div[2]/app-publication-card[1]/mat-card/app-article-metadata/div[1]/div[2]/div/div').text
        self.title_timescited[a] = b


    def results_page(self):
        self.driver.find_element_by_xpath('//*[@id="snProfilesSearchTop"]/mat-card/div[2]/mat-card-title/h3/a/span').click()
        print("저자검색 리스트중 첫번째 저자 클릭")
        time.sleep(5)



    def author_crwal(self):
        self.driver.get(self.url)
        self.driver.implicitly_wait(3)
        print("저자검색 접속 완료")
        self.driver.find_element_by_xpath('/html/body/app-wos/div/div/main/div/app-input-route/app-input-route/app-search-home/div[2]/div/app-author-search/div/div[1]/app-author-search-select/wos-select').click()
        print("검색방법 선택 1")
        time.sleep(7)
        self.driver.find_element_by_xpath('//*[@id="pendo-close-guide-8fdced48"]').click()
        print("팝업창 닫기")
        time.sleep(2)
        self.driver.find_element_by_xpath('/html/body/app-wos/div/div/main/div/app-input-route/app-input-route/app-search-home/div[2]/div/app-author-search/div/div[1]/app-author-search-select/wos-select').click()
        print("검색방법 선택 2")
        time.sleep(2)
        self.driver.find_element_by_xpath('//*[@id="global-select"]/div[1]/div[2]/div[2]').click()
        print("저자이이디 검색 클릭")
        time.sleep(2)
        self.driver.find_element_by_xpath('//*[@id="orcid"]').send_keys('H-5749-2019')
        # AAN-4311-2021 AAN-7740-2021 AAQ-5923-2021 H-5749-2019 G-6114-2017
        print("저자아이디 입력")
        time.sleep(5)
        self.driver.find_element_by_xpath('/html/body/app-wos/div/div/main/div/app-input-route/app-input-route/app-search-home/div[2]/div/app-author-search/div/div/div/app-rid-search-form/div/button[2]/span[1]').click()
        print("저자검색 클릭")
        time.sleep(7)

        self.now_url = self.driver.current_url
        print(self.now_url)
        if "results" in self.now_url:
            self.results_page()
            self.author_page()
        else:
            self.author_page()




        #  self.driver.quit()




a = WOS_author()
a.author_crwal()
