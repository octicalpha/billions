###Database Table Must be created beforehand###


import psycopg2

def csv_to_db_transfer(csv_file, cur, db_table):

   opened_csv_file = open(csv_file, 'r')
   opened_csv_file.seek(0)
   copy_sql = """
              COPY """ + db_table + """ FROM stdin WITH CSV HEADER
              DELIMITER as ','
              """
   try:
       cur.copy_expert(sql=copy_sql,file=opened_csv_file)
   except Exception as csv_to_db_exception:
       print csv_to_db_exception
       
if __name__ == '__main__':
    conn = psycopg2.connect(os.environ["Database'])
    cur = conn.cursor()
    # enter the full path in the input file
    input_file = ""
    #Modify the table name you want to input
    csv_to_db_transfer(input_file, cur, 'gdax."BTC-USD_Trades"')
    conn.commit()
    cur.close()
