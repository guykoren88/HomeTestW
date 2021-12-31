import requests
import pandas as pd
import json
import pymysql
import sqlite3
from pandas import json_normalize
from sqlalchemy import create_engine
import urllib3
from urllib.parse import quote

## Q1
url = "https://randomuser.me/api/?results=4500"

response = requests.get(url)
dictr = response.json()
recs = dictr['results']
df = json_normalize(recs)

## Q2
MaleTable = df[df['gender']=='male']
FemaleTable = df[df['gender']=='female']

## connection to MySQL
connection = pymysql.connect(host='104.197.7.195',
                             user='interview_user',
                             password='aviv_2021_07_06_!!@@QQ',
                             db='interview')

cursor = connection.cursor()

engine = create_engine("mysql+pymysql://interview_user:%s@104.197.7.195/interview" % quote('aviv_2021_07_06_!!@@QQ'))

## Create tables and Insert data

MaleTable.to_sql('Guy_Koren_test_Male', con = engine, if_exists = 'replace', chunksize = 1000)
FemaleTable.to_sql('Guy_Koren_test_Female', con = engine, if_exists = 'replace', chunksize = 1000)

## Q3
## In the next question I used loop for create the dataset

df10 = df[(df["dob.age"] >= 0)  & (df["dob.age"] < 10)]
df20 = df[(df["dob.age"] >= 10) & (df["dob.age"] < 20)]
df30 = df[(df["dob.age"] >= 20) & (df["dob.age"] < 30)]
df40 = df[(df["dob.age"] >= 30) & (df["dob.age"] < 40)]
df50 = df[(df["dob.age"] >= 40) & (df["dob.age"] < 50)]
df60 = df[(df["dob.age"] >= 50) & (df["dob.age"] < 60)]
df70 = df[(df["dob.age"] >= 60) & (df["dob.age"] < 70)]
df80 = df[(df["dob.age"] >= 70) & (df["dob.age"] < 80)]
df90 = df[(df["dob.age"] >= 80) & (df["dob.age"] < 90)]
df100 = df[(df["dob.age"] >= 90) & (df["dob.age"] <= 100)]


## Q4
for i in range(10, 110, 10):
    TableName = 'Guy_Koren_test_'+str(i)+'s'
    dfGroup = 'df'+str(i)
    Group1 = df[(df["dob.age"] >= i - 10 ) & (df["dob.age"] < i)]
    Group2 = df[(df["dob.age"] >= i - 10 ) & (df["dob.age"] <= i)]
    if i < 100:
        Group1.to_sql(TableName, con=engine, if_exists='replace', chunksize=1000)
    else:
        Group2.to_sql(TableName, con=engine, if_exists='replace', chunksize=1000)


## Q5

MaleTop20Registered = MaleTable.sort_values(by='registered.date', ascending=False).head(20)
FemaleTop20Registered = FemaleTable.sort_values(by='registered.date', ascending=False).head(20)

## Union All
UnionMandf20 = pd.concat([MaleTop20Registered, FemaleTop20Registered])


UnionMandf20.to_sql('Guy_Koren_test_20', con = engine, if_exists = 'replace', chunksize = 1000)

## Q6

## Union
CombineData20and5 = pd.concat([UnionMandf20, df50]).drop_duplicates()

CombineData20and5.to_json(r'/Users/guykoren/Desktop/First.json')


## Q7

CombineData20and2 = pd.concat([UnionMandf20, df20])

CombineData20and2.to_json(r'/Users/guykoren/Desktop/Second.json')


connection.commit()




