from os import replace
from pymongo import MongoClient


client = MongoClient('mongodb://203.255.92.141:27017', authSource='admin')

def __main__():
    input_keyid = 1254
    input_site = ["SCIENCEON", "KCI", "NTIS"]
    a = crawling_name_inst()
    a.author_crawl(input_keyid, input_site)
    
     
class crawling_name_inst:
    def __init__(self):
        self.client = MongoClient('mongodb://203.255.92.141:27017', authSource='admin')
        
         
    def author_crawl(self, keyid, site):
        cand = {}
        
        self.file_data = open(f'answer{keyid}.csv', 'w', encoding='utf-8-sig')
        for j in site:
            len_result = 0
            
            for i in self.client[j]['Rawdata'].find({"keyId":keyid}):
                if j != 'NTIS' and i['originalName'] not in "":
                    paper_inst = i['originalName'].split(';')[-2]
                    mng_name = i['author'].split(';')[-2]
                    len_result += 1
                    x = mng_name.replace(',', '^') + ',' + j + ',' + paper_inst.replace(',', ';') + '\n'
                    if mng_name not in cand:
                        cand[mng_name] = []
                        
                    cand[mng_name].append(x)
                elif i['originalName'] not in "" :
                    paper_inst = i['originalName']
                    mng_name = i['mng']
                    len_result += 1
                    x = mng_name.replace(',', '^') + ',' + j + ',' + paper_inst.replace(',', ';') + '\n'
                    if mng_name not in cand:
                        cand[mng_name] = []
                        
                    cand[mng_name].append(x)
                    
        for cand_one in cand.values():
            cand_two = set(cand_one)
            
            if len(cand_two) >= 2:
                for c in cand_two:
                    self.file_data.write(c)
        self.file_data.close()
        print(site, "끝")

__main__()