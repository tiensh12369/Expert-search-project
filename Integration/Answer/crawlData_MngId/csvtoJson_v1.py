import csv
from numpy import double
import json
from collections import OrderedDict
from numpy.lib.function_base import append
import pandas as pd
from pandas.io.pytables import AttributeConflictWarning

# f = open('answer777.csv', 'r')
f = open('answer995.csv', 'r', encoding="utf-8")
rdr = csv.reader(f)
file_data = OrderedDict()
site = ['SCOPUS', 'WOS']

for line in rdr:
    # 이름 = key, 나머지 데이터를 value (site, 소속)
    #print(line)
    #file_data[line[0]] = {line[1] : line[2]}
    # print(line)
    data = {}
    for idx in range(len(line)) :
        #print(idx, line[idx])
        # print(idx)
        if line[idx] in site:
            data[line[idx]]=line[idx+1].replace(";", ",")
    # print(line)
    # print(data)
    file_data[line[0]]=data


with open('answer995.json', 'w', encoding='utf-8') as f :
# with open('answer777.json', 'w', encoding='euc-kr') as f :
    json.dump(file_data, f, ensure_ascii=False)

# file_data["name"] = "C"
print(json.dumps(file_data, ensure_ascii=False))
# print(rdr)