
import mongo_connection
import pandas as pd

mydb = mongo_connection.myclient.nse_database
symbol_series = mydb.symbol_series
company_details = mydb.company_details

files_list = ['june10_2019.csv', 'june1_2019.csv']

while True:
    newfile = input("The file name: ")
    if newfile in files_list:
        print("File already inserted")
    else:
        file_read = pd.read_csv(newfile)
        records = file_read. to_dict(orient='records')
        #result = mydb.mycol1.insert_many(records)
        #print(records)
        table_1 = []
        for rec in records:
            table_1.append({'SYMBOL' : rec['SYMBOL'], 'SERIES' : rec['SERIES']})
            #print(table_1)
        result1 = mydb.symbol_series.insert_many(table_1)
        table_2 = []
        for rec in records:
            table_2.append({'DATE1' : rec['DATE1'], 'PREV_CLOSE' : rec['PREV_CLOSE'],
                            'OPEN_PRICE' : rec['OPEN_PRICE'],'HIGH_PRICE' : rec['HIGH_PRICE'],
                            'LOW_PRICE' : rec['LOW_PRICE'],'LAST_PRICE' : rec['LAST_PRICE'],
                            'CLOSE_PRICE' : rec['CLOSE_PRICE'],'AVG_PRICE' : rec['AVG_PRICE'],
                            'TTL_TRD_QNTY' : rec['TTL_TRD_QNTY'],'TURNOVER_LACS' : rec['TURNOVER_LACS'],
                            'NO_OF_TRADES' : rec['NO_OF_TRADES'],'DELIV_QTY' : rec['DELIV_QTY'],
                            'DELIV_PER' : rec['DELIV_PER']})
            #print(table_2)
        result2 = mydb.company_details.insert_many(table_2)
    break
