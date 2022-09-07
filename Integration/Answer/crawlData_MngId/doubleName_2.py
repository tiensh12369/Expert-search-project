import csv
from numpy import double

from pandas.io.pytables import AttributeConflictWarning
f = open('answer995.csv', 'r', encoding='UTF-8') # euc-kr
# f = open('answer800.csv', 'r') # euc-kr
# f = open('answer792.csv', 'r')
rdr = csv.reader(f)

temp_author = ""
temp_flag = False # 출력했을 때 True로 바꿔주는것
temp_count = 0 # 같을 때 +1 씩 추가하기 위한것

for line in rdr:
    # print(line[0])
    author = line[0] #author = 고수정
    if author != temp_author :
        temp_author = author
        temp_flag = False
    else : #같다면 author == temp_author  
        temp_count += 1
        # print(temp_count)
        if temp_flag == False :
            print(temp_author)
            temp_flag = True
            
        
        # temp_count += 1
        # print(temp_count)
        # count = []Sriboonchitta Songsak
        # doublename = [temp_author]
        # for i in doublename:
        #     try: count[i] += 1
        #     except: count[i]=1
        # print(count)
        
        # temp_count += 1
        # print(temp_count)
            
        # cnt = line.count(temp_author)
        # print(cnt)
    
    #author = line[0]
    #temp_author = author
    
    

    # for i in author:
    # if author != temp_author:
    #     temp_author 
    #     temp_count += (temp_count+1)

print(temp_count)
f.close()

