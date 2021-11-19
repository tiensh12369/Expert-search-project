import json

answer = None
numCor = 0

def ansCheck(result, name) :
    global answer, numCor
    flag = True
    numK = len(test.keys())
    for site in test.keys() :
        if site != 'name' :
            if numK > 2:
                if answer[name][site] != test[site].split(' ')[0] :
                    flag = False
            else :
                if answer[name][site] != test[site] :
                    flag = False
    if flag :
        numCor += 1
    else :
        print(f"Not Correct \n - Answer : {answer[name]}\n - Result : {test}")
    return flag

with open('answer.json', 'r',encoding='UTF8') as a_json :


    answer = json.load(a_json)
    numAns = print(len(answer))
    test = {'name' : '유재수','NTIS': '충북대학교', 'DBPIA': '충북학교', 'Scienceon': '충북대학교 정보통신공학'}
    test = {'name' : '한정훈', 'DBPIA': '한국농촌경제연구원'}
    # test = {'name' : '한정훈', 'DBPIA': '한국사회체육학회'}


    name = test['name']
    if name in answer :
        ansCheck(test, name)

    else :
        count = 0
        while name+str(count) in answer :
            # if name+count in answer :
            print(f"Check {name+str(count)}")
            if ansCheck(test, name+str(count)) :
                break
            count += 1

    print(numCor)
