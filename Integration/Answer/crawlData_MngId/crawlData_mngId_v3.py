from os import replace
from pymongo import MongoClient

client = MongoClient('mongodb://203.255.92.141:27017', authSource='admin')

def __main__():
    input_keyid = 1226
    input_site = ["SCIENCEON", "KCI", "NTIS"]
    a = crawling_name_inst()
    a.author_crawl(input_keyid, input_site)
    
from itertools import combinations

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
                    
        cand_dict= {}
        for cand_one in cand:
            cand_value = list(set(cand[cand_one]))
            mng_name = cand_one
            
            if mng_name not in cand_dict:
                cand_dict[mng_name] = []
                
            if len(cand_value) >= 2:
                cand_three = []
                integration_dict= {}
                
                j = 1
                
                for c in cand_value:
                    cand_three.append([c, j])
                    j += 1
                
                for a in cand_three:
                    cand_check = [a[1]]
                    for n in cand_three:
                        if a == n:
                            break
                        cand_inst1 = a[0].split(',')[2]
                        cand_inst2 = n[0].split(',')[2]
                        cand_data2 = n[0].split(',')[1] + ',' + cand_inst2
                        
                        if cand_inst1 == cand_inst2 and n[1] not in cand_check:
                            cand_check.append(n[1])
                            integration_dict[a[0][:-1] + ',' + cand_data2] = cand_check
                
                cand_dict[mng_name].extend(integration_dict)

            else:
                cand_dict[mng_name].extend(cand_value)

        for cand_v in cand_dict.values():
            for i in cand_v:
                self.file_data.write(i)

        self.file_data.close()
        print(site, "끝")
__main__()