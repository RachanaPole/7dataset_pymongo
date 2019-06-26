import pymongo
import pandas as pd

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["mydatabase"]
mycol1 = mydb["alldata"] #collection containing all the details of each company
mycol2 = mydb["company"] #collection with symbol and series
df1 = pd.read_csv(r"C:\Users\poler\Downloads\f1.csv")
records1 = df1. to_dict(orient='records')
result1 = mydb.mycol1. insert_many(records1)
df2 = pd.read_csv(r"C:\Users\poler\Downloads\f2.csv")
records2 = df2. to_dict(orient='records')
result2 = mydb.mycol1. insert_many(records2)
df3 = pd.read_csv(r"C:\Users\poler\Downloads\f3.csv")
records_3 = df3. to_dict(orient='records')
result3 = mydb. mycol1. insert_many(records_3)
df4 = pd.read_csv(r"C:\Users\poler\Downloads\f4.csv")
records4 = df4. to_dict(orient='records')
result4 = mydb. mycol1. insert_many(records4)
df5 = pd.read_csv(r"C:\Users\poler\Downloads\f5.csv")
records5 = df5. to_dict(orient='records')
result5 = mydb. mycol1. insert_many(records5)
df6 = pd.read_csv(r"C:\Users\poler\Downloads\f6.csv")
records6 = df6. to_dict(orient='records')
result6 = mydb. mycol1. insert_many(records6)
df7 = pd.read_csv(r"C:\Users\poler\Downloads\f7.csv")
records7 = df7. to_dict(orient='records')
result7 = mydb.mycol1. insert_many(records7)

print(records1)

table_2 = []
for rec in records1:
    table_2.append({'SYMBOL' : rec['SYMBOL']})
    table_2.append({'SERIES' : rec['SERIES']})
print(table_2)
result2 = mydb.mycol2.insert_many(table_2)


