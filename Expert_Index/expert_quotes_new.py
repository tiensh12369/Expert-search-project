#!/usr/bin/env python
# coding: utf-8

# In[4]:


import json

def calculate_h_index(quotation): #h_ndex
    quotation.sort(reverse=True)  # 논문을 내림차순으로 정렬

    h_index = 0
    for i, quotation in enumerate(quotation):
        if quotation >= i + 1:
            h_index = i + 1
        else:
            break

    return h_index


def calculate_i10_index(quotation):  #i10_index
    i10_index = sum(1 for quotation in quotation if quotation >= 10)
    return i10_index


def calculate_g_index(quotation):  #g_index
    quotation.sort(reverse=True)  # 논문을 내림차순으로 정렬

    g_index = 0
    quotation_sum=0
    for i, quotation in enumerate(quotation):
        quotation_sum+=quotation
        if quotation_sum >= (i + 1)**2:
            g_index = i + 1

    return g_index






# In[5]:


import pandas as pd

#authors.all 파일 위치 주소
authors_file_path = r"path_to_authors.all"

try:
    with open(authors_file_path, 'r', encoding='cp1252') as file:
        lines = file.read().splitlines()
        data = [line.split('|') for line in lines]
        columns = ["Valid", "Name", "Affiliation", "Email Domain", "Citation Count", "ID", "C6", "C7"]
        df_a = pd.DataFrame(data,columns=columns) # authors 의 a
        df_a = df_a.drop(columns=["C6", "C7"])
        print("변환 완료")
except FileNotFoundError:
    print("파일을 찾을 수 없습니다.")
except UnicodeDecodeError:
    print("파일을 ANSI(Windows-1252) 인코딩으로 열 수 없습니다.")


# In[7]:


#classes_authors.all 파일 위치 주소
classes_authors_file_path = r"path_to_classes_authors.all"

try:
    with open(classes_authors_file_path, 'r', encoding='cp1252') as file:
        data = file.readlines()

    # 모든 태그를 저장할 리스트를 초기화합니다.
    all_tags = []

    # 각 줄에서 태그를 추출합니다.
    for line in data:
        _, tags = line.strip().split("|")
        tags = tags.split(";;;")
        all_tags.extend(tags)

    # 중복 제거를 위해 set로 변환한 후 다시 리스트로 변환합니다.
    unique_tags = list(set(all_tags))

    # 고유한 태그의 개수를 확인합니다.
    tag_count = len(unique_tags)

    # 출력
    print(f"고유한 태그의 개수: {tag_count}")
        
except FileNotFoundError:
    print("파일을 찾을 수 없습니다.")
except UnicodeDecodeError:
    print("파일을 ANSI(Windows-1252) 인코딩으로 열 수 없습니다.")


# In[10]:


#Object 타입에서 숫자형 체크용
def is_numeric(value):
    try:
        float(value)  # float으로 변환 가능한 경우 숫자
        return True
    except (ValueError, TypeError):
        return False


# In[11]:


df_a_copy = df_a.copy()  # df_a의 복사본 생성

for index, row in df_a_copy.iterrows():
    if pd.isna(row[5]):  
        if pd.isna(row[4]):
            if pd.isna(row[3]):  # 1의 경우
                row[5] = row[2] 
            else:  # 2,3,4 의 경우
                row[5] = row[3]
                if is_numeric(row[2]): # 4의 경우
                    row[4] = row[2]
                    row[2] = None
                    row[3] = None
                elif '.' in str(row[2]):  # 3의 경우
                    row[3] = row[2]
                    row[2] = None
                    row[4] = None
                else: # 2의 경우
                    row[3] = None
        else:  # 5,6,7 의 경우
            row[5] = row[4] 
            if is_numeric(row[3]): # 6,7 의 경우
                row[4] = row[3]
                if '.' in str(row[2]): # 7 의 경우
                    row[3] = row[2]
                    row[2] = None
                else: # 6의 경우
                    row[3] = None
            else: #5의 경우
                row[4] = None
                
#1,2,3,4,5,6,7은 각각 소속/이메일 도메인/인용수 중 하나 또는 그 이상 없는 경우에 해당
# 번호 / 소속 / 이메일 도메인 / 인용수
# 1 / X / X /X 
# 2 / 0 / X /X 
# 3 / X / 0 /X 
# 4 / X / X /0 
# 5 / 0 / 0 /X 
# 6 / 0 / X /0 
# 7 / X / 0 /0 


# In[12]:


import os

#gsc_data\DATA 폴더 경로
folder_path = r"path_to ~\expert\gsc_data\DATA"

# df_a_copy의 각 행에서 1번 열의 값을 추출
for index, row in df_a_copy.iterrows():
    name = row[5]  # 1번 열의 값
    
    # 파일 경로 생성
    file_path = os.path.join(folder_path, name + "_.dat")
    
    # 파일이 존재하는지 확인
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as file:
            # 파일의 각 줄을 읽어 리스트로 저장
            lines = file.readlines()

        # 각 줄을 나누어서 리스트로 저장
        line_lists = [line.strip().split('|') for line in lines]
        
        #빈 리스트 생성
        first_column_values = []

        for line_list in line_lists:
            if len(line_list) > 0:
                first_column_values.append(int(line_list[0]))
         
        #각 index 계산 후 df_a_copy에 새로운 열로 추가
        df_a_copy.at[index,"h-index"] = calculate_h_index(first_column_values)
        df_a_copy.at[index,"i10-index"] = calculate_i10_index(first_column_values)
        df_a_copy.at[index,"g-index"] = calculate_g_index(first_column_values)

    else:
        print(f"파일을 찾을 수 없습니다: {file_path}")


# In[14]:


df_a_copy.head(20)


# In[ ]:


#결과 CSV 로 저장
df_a_copy.to_csv('output.csv', index=False)
df_a_copy.to_csv('output_index.csv', index=True)

