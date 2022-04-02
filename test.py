'''
TODO:
1. DEX 서비스 추가하면서 시간 얼마나 걸리는지 측정
    1) 의미없는 반복작업 최소화해서 성능 향상 (추후에 서비스 많아지면 성능 이슈 생길듯)
2. 토큰 가격 정보 시간단위(1분? 10분?)로 저장해서 추후에 차트 혹은 토큰 가격 추세 자료에 사용할 수 있도록 수정
3. 예외처리구문 추가 및 예외발생시 텔레그램으로 로그 보내도록 수정
4. 30일 지난 데이터는 DB에서 삭제하는 로직 추가
5. 서비스별 총 TVL 계산해서 저장하는 로직 추가
6. Pool LP TVL이 일정 기준치 미달되는건 무시하도록 로직 추가
7. 풀에 포함된 토큰 중 토큰 주소 없는 경우 로그

Q. LP POOL을 구성하는 토큰을 한번에 요청하는 것이 아니라 각각 요청하기 때문에 변경된거가 반영 안됐을 수도 있는데 데이터 정확성 보장은 어떻게 해야되지?
Q. 크론잡 외에 주기적으로 실행되도록 할 수 있나?
Q. KAS 계정 추가해서 API 무료 호출 횟수 올리고 무료 횟수 다 쓰면 계정 돌려쓸 수 있도록 가능한가?
Q. 가스비 어떻게 계산?

'''
import math
import time
import sys
import requests
import json
import constant
import datetime
from pymongo import MongoClient

#코인원 클레이튼 API
priceKlayUrl = 'https://api.coinone.co.kr/ticker/?currency=KLAY'

#가스비 (평균치로 샘플로 등록, 가스비 정보 가져올 수 있나?)
gasFee = 0.02

mongoClient = MongoClient("localhost:27017", maxPoolSize=500)
database = mongoClient["test"]

# app info
application_info_collection = database["application_info"]

# service list
update_date = database["update_date"]

# service list
service_collection = database["service"]

# token price
token_price_collection = database["token_price"]
token_price_data_collection = database["token_price_data"]

# pool info
# klayswap_pool_collection = database["klayswap_pool"]
# klaymore_pool_collection = database["pala-pool"]

def getKlayPrice():
    try:
        response = requests.get(priceKlayUrl)
        return response.json()['last']
    except Exception as ex:
        print("Exception in function 'getKlayBalance': " + str(ex))
        return None

#set swap_rate
def setSwapRate():
    try:
        swapRate = {}
        service_list = service_collection.find()
        for service in service_list:
            serviceName = service['name']
            pool_collection = database[serviceName+"_pool"]
            pool_info = pool_collection.find()
            swapRate[serviceName] = {}
            for info in pool_info:
                tokenA_name = info['tokenA_name']
                tokenB_name = info['tokenB_name']
                tokenA_cnt = info['tokenA_cnt']
                tokenB_cnt = info['tokenB_cnt']
                data = {
                    tokenA_name: tokenA_cnt,
                    tokenB_name: tokenB_cnt
                }
                pool_name1 = tokenA_name + '-' + tokenB_name
                pool_name2 = tokenB_name + '-' + tokenA_name
                swapRate[serviceName][pool_name1] = data
                swapRate[serviceName][pool_name2] = data
        return swapRate
    except Exception as ex:
        print("Exception in function 'getKlayBalance': " + str(ex))
        return None

def setPoolList():
    try:
        poolList = {}
        serviceList = service_collection.find()
        for service in serviceList:
            serviceName = service['name']
            pool_collection = database[serviceName+"_pool"]
            pool_info = pool_collection.find()
            poolList[serviceName] = {}
            for info in pool_info:
                tokenA_name = info['tokenA_name']
                tokenB_name = info['tokenB_name']
                if tokenA_name not in poolList[serviceName].keys():
                    poolList[serviceName][tokenA_name] = set([tokenB_name])
                else:
                    poolList[serviceName][tokenA_name].add(tokenB_name)
                if tokenB_name not in poolList[serviceName].keys():
                    poolList[serviceName][tokenB_name] = set([tokenA_name])
                else:
                    poolList[serviceName][tokenB_name].add(tokenA_name)
        return poolList
    except Exception as ex:
        print("Exception in function 'getKlayBalance': " + str(ex))
        return None

#토큰 swap 경로 탐색
def dfs(serviceName, poolList, tokenA, tokenB):
    try:
        stack = [(tokenA, [tokenA])]
        result = []

        while stack:
            n, path = stack.pop()
            if n == tokenB:
                result.append(path)
            else:
                for m in poolList[serviceName][n] - set(path):
                    stack.append((m, path + [m]))
        return result
    except Exception as ex:
        print("Exception in function 'getKlayBalance': " + str(ex))
        return None

#전에 계산된 토큰 가격 있으면 해당 가격 이용하도록 메모로직 추가
def calculatePrice(serviceName, pathList, swapRate):
    try:
        result = 0
        if pathList == None:
            return 0
        else:
            for path in pathList:
                temp = float(getKlayPrice())
                for i in range(0, len(path)-1):
                    pool_name = path[i] + '-' + path[i+1]
                    temp = temp * (swapRate[serviceName][pool_name][path[i]] / swapRate[serviceName][pool_name][path[i+1]])
                result = result + temp
                # print(str(temp))
                # print(path)
                # print("\n")
                # if(result < temp):
                #     result = temp
            # result = result/len(path_list)
            return result/len(pathList)
    except Exception as ex:
        print("Exception in function 'getKlayBalance': " + str(ex))
        return 0

def getKlayBalance(address):
    try:
        header = {
            'x-chain-id':
                '8217',
            'Authorization':
                'Basic S0FTSzJIMFZXVzE4NjVLT002RlJQUkVWOmQ4dkR2MjlQWWhDcGx5TWhtRU5HMV9tVUdrWElqbTY0cGw2Nmh0NHI='
        }
        url = 'https://node-api.klaytnapi.com/v1/klaytn'
        data_raw = {
            "jsonrpc": "2.0",
            "method": "klay_getBalance",
            "params": [address, 'latest'],
            "id": 1
        }
        responses = requests.post(url, headers=header, json=data_raw)
        if responses.status_code == 200:
            return int(responses.json()['result'], 16) * 0.1e-17
        else:
            return -1
    except Exception as ex:
        print("Exception in function 'getKlayBalance': " + str(ex))
        return -1

def getTokenBalance(tokenAddress, poolAddress):
    try:
        header = {
            'x-chain-id':
                '8217',
            'Authorization':
                'Basic S0FTSzJIMFZXVzE4NjVLT002RlJQUkVWOmQ4dkR2MjlQWWhDcGx5TWhtRU5HMV9tVUdrWElqbTY0cGw2Nmh0NHI='
        }
        url = 'https://kip7-api.klaytnapi.com/v1/contract/' + tokenAddress + '/account/' + poolAddress + '/balance'

        responses = requests.get(url, headers=header)
        if responses.status_code == 200:
            decimal = responses.json()["decimals"]
            balance = responses.json()["balance"]
            return int(balance, 16) * (0.1**decimal)
        else:
            return -1
    except Exception as ex:
        print("Exception in function 'getTokenBalance': " + str(ex))
        return -1

def updateDB_poolInfo():
    try:
        for serviceName in constant.POOL_CONTRACT_ADDRESS:
            pool_collection = database[serviceName+"_pool"]
            for pool_info in constant.POOL_CONTRACT_ADDRESS[serviceName]:
                poolAddress = constant.POOL_CONTRACT_ADDRESS[serviceName][pool_info]

                token = pool_info.split('-')
                tokenA = token[0]
                tokenB = token[1]

                data = {}
                if tokenA == 'KLAY':
                    tokenBAddress = constant.TOKEN_ADDRESS[tokenB]
                    data['tokenA_name'] = tokenA
                    data['tokenB_name'] = tokenB
                    data['tokenA_cnt'] = getKlayBalance(poolAddress)
                    data['tokenB_cnt'] = getTokenBalance(tokenBAddress, poolAddress)

                elif tokenB == 'KLAY':
                    data['tokenA_name'] = tokenA
                    data['tokenB_name'] = tokenB
                    tokenAAddress = constant.TOKEN_ADDRESS[tokenA]
                    data['tokenB_cnt'] = getKlayBalance(poolAddress)
                    data['tokenA_cnt'] = getTokenBalance(tokenAAddress, poolAddress)

                elif tokenA == 'WKLAY':
                    data['tokenA_name'] = tokenA
                    data['tokenB_name'] = tokenB
                    tokenAAddress = constant.TOKEN_ADDRESS[tokenA][serviceName]
                    tokenBAddress = constant.TOKEN_ADDRESS[tokenB]
                    data['tokenA_cnt'] = getTokenBalance(tokenAAddress, poolAddress)
                    data['tokenB_cnt'] = getTokenBalance(tokenBAddress, poolAddress)

                elif tokenB == 'WKLAY':
                    data['tokenA_name'] = tokenA
                    data['tokenB_name'] = tokenB
                    tokenAAddress = constant.TOKEN_ADDRESS[tokenA]
                    tokenBAddress = constant.TOKEN_ADDRESS[tokenB][serviceName]
                    data['tokenA_cnt'] = getTokenBalance(tokenAAddress, poolAddress)
                    data['tokenB_cnt'] = getTokenBalance(tokenBAddress, poolAddress)

                else:
                    data['tokenA_name'] = tokenA
                    data['tokenB_name'] = tokenB
                    tokenAAddress = constant.TOKEN_ADDRESS[tokenA]
                    tokenBAddress = constant.TOKEN_ADDRESS[tokenB]
                    data['tokenA_cnt'] = getTokenBalance(tokenAAddress, poolAddress)
                    data['tokenB_cnt'] = getTokenBalance(tokenBAddress, poolAddress)

                pool_collection.update_one({'name': tokenA+'-'+tokenB}, {'$set': data}, upsert=True)
                pool_collection.update_one({'name': tokenB+'-'+tokenA}, {'$set': data}, upsert=True)
    except Exception as ex:
        print("Exception in function 'updateDB_poolInfo': " + str(ex))

def updateDB_tokenPrice():
    #TODO: tokenPrice가 아니라 서비스에 포함된 코인 가격만 계산하도록 수정
    # klay = getKlayPrice()
    try:
        # time = datetime.datetime.strftime("%Y-%m-%d %H:%M:%S")
        time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        swapRate = setSwapRate()
        poolList = setPoolList()
        priceList = {}
        for serviceName in constant.SERVICE_LIST:
            keyToken = constant.SERVICE_LIST[serviceName]['keyToken']
            tokenList = []
            for pool_info in constant.POOL_CONTRACT_ADDRESS[serviceName]:
                token = pool_info.split('-')
                if token[0] not in tokenList:
                    tokenList.append(token[0])
                if token[1] not in tokenList:
                    tokenList.append(token[1])
            for tokenName in tokenList:
                price = calculatePrice(serviceName, dfs(serviceName, poolList, keyToken, tokenName), swapRate)
                tokenPrice = setPriceDigit(price)

                if tokenName not in priceList.keys():
                    priceList[tokenName] = {}
                priceList[tokenName][serviceName] = tokenPrice

        tokenPriceData = {}
        for priceData in priceList:
            total = 0
            for price in priceList[priceData].values():
                total += price
            tokenPriceData[priceData] = setPriceDigit(total/len(priceList[priceData]))
            token_price_collection.update_one({'name': priceData}, {'$set': {'price': priceList[priceData]}}, upsert=True)

        token_price_data_collection.insert_one({'date': time, 'tokenPriceData': tokenPriceData})

    except Exception as ex:
        print("Exception in function 'updateDB_tokenPrice': " + str(ex))

def setPriceDigit(price):
    if price > 100:
        return round(price)
    elif price > 1:
        return round(price, 2)
    else:
        return round(price, 4)

'''
최초 실행시 Mongodb Setting
constant 업데이트시 DB 반영
'''
def init():
    try:
        constant_update_date = constant.UPDATE_DATE
        application_info = application_info_collection.find_one({'name': 'application_info'})
        if application_info == None:
            data = {
                "name": 'application_info',
                "update_date": constant_update_date,
            }
            application_info_collection.insert_one(data)
        else:
            if application_info['update_date'] == constant_update_date:
                print('already update db todo: logging')
                return
            else:
                application_info_collection.update_one({"name": 'application_info'}, {"$set": {"update_date": constant_update_date}}, upsert=True)
                print('update db todo: logging')

        #연동된 서비스 DB에 입력 (constant.py에 등록된 서비스)
        for serviceName in constant.SERVICE_LIST:
            result = service_collection.find_one({'name': serviceName})
            if result == None:
                service_collection.insert_one({'name': serviceName})

        for tokenName in constant.TOKEN_ADDRESS:
            result = token_price_collection.find_one({'name': tokenName})
            if result == None:
                token_price_collection.insert_one({'name': tokenName, 'price': 0})

        #풀에 포함되어 있는 Token DB에 입력 -> TOKEN_ADDRESS에 있는 정보로 대체
        # for service in constant.POOL_CONTRACT_ADDRESS:
        #     for pool_info in constant.POOL_CONTRACT_ADDRESS[service]:
        #         token = pool_info.split('-')
        #         result = token_price_collection.find_one({'name': token[0]})
        #         if(result == None):
        #             token_price_collection.insert_one({'name': token[0], 'price': 0})
        #         result = token_price_collection.find_one({'name': token[1]})
        #         if(result == None):
        #             token_price_collection.insert_one({'name': token[1], 'price': 0})
    except Exception as ex:
        print("Exception in function 'init': " + str(ex))

if __name__ == '__main__':

    init()

    start = time.time()
    updateDB_poolInfo()
    end = time.time()
    print("updateDB_poolInfo() : " + f"{end - start:.2f}sec")

    #ser token price
    start = time.time()
    updateDB_tokenPrice()
    end = time.time()
    print("updateDB_tokenPrice() : " + f"{end - start:.2f}sec")
