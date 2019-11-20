"""
THIS SCRIPT EXTRACTS NEW YORK TIMES DATA THROUGH API NYT DEVELOPER WEBSITE FOR A GIVEN MONTH AND YEAR
https://developer.nytimes.com/

Available data for all publishing period: 1851-present.

To access the data you need register on the website and get an API KEY!

Example of meta data for a month:
    Note! You need to substitute your own key on YOUR_UNIQUE_KEY_FROM_DEVELOPER_NYT_WEBSITE
    
    https://api.nytimes.com/svc/archive/v1/2000/1.json?api-key=YOUR_UNIQUE_KEY_FROM_DEVELOPER_NYT_WEBSITE
"""

from urllib.request import urlopen
import json
import pymysql
import time
import string

#---------------------------------------------
#Declare which year's and month's data you want to collect
year = '2014'
month = '9'
#---------------------------------------------

API_KEY = 'YOUR_UNIQUE_KEY_FROM_DEVELOPER_NYT_WEBSITE'
link_part_1 = 'https://api.nytimes.com/svc/archive/v1/'
link_part_2 = '.json?api-key=' + API_KEY +'#'

middle = year + '/' + month

#get data from NYT using API
def get_data_month (url):
    data = urlopen(url, None).read().decode("utf-8")
    return json.loads(data)

print('Start.')

#saving data from NYT in this variable
data = get_data_month(link_part_1 + middle + link_part_2)

#CONNECT TO MYSQL DATABASE
conn = pymysql.connect(db="database_name",
                       user="user_name_for_database",
                       passwd="password_to_database",
                       host= "host", 
                       port = 3306 #usually for MySQL
                       )
print('Connected to MySQL.')

cursor = conn.cursor()

sql_query_select_id_issue = "SELECT id_issue FROM DT_NYT.NYT_ISSUE WHERE issue_date = %s;"
sql_query_select_id_document_type = "SELECT id_document_type FROM DT_NYT._DOCUMENT_TYPE WHERE name_document_type = %s;"
sql_query_select_id_type_of_material = "SELECT id_type_of_material FROM DT_NYT._TYPE_OF_MATERIAL WHERE name_type_of_material = %s;"
sql_query_select_id_news_desk = "SELECT id_news_desk FROM DT_NYT._NEWS_DESK WHERE name_news_desk = %s;"
sql_query_select_id_section_name = "SELECT id_section_name FROM DT_NYT._SECTION_NAME WHERE name_section_name = %s;"

sql_query_insert = "INSERT INTO DT_NYT.NYT_ARTICLE_INFO_" + month + \
                   " (`id_issue`, `web_url`, `snippet`, `lead_paragraph`, `title`, `word_count`, `id_type_of_material`," + \
                   " `id_document_type`, `doc_id`, `abstract`) " + \
                   "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"

t = time.time()

print('Number of articles in {}/{} is {}'.format(year, month, data['response']['meta']['hits']))

for i in range(0, len(data['response']['docs'])):    
    
    cursor.execute(sql_query_select_id_issue, data['response']['docs'][i]['pub_date'][:10])
    id_issue = cursor.fetchall()
    print(i, data['response']['docs'][i]['pub_date'], id_issue[0])
    
    #working on document_type
    cursor.execute(sql_query_select_id_document_type, data['response']['docs'][i]['document_type'])
    id_document_type = cursor.fetchall()
    print(data['response']['docs'][i]['document_type'], id_document_type[0])
    
    #working on type_of_material
    temp = 0
    
    if data['response']['docs'][i]['type_of_material'] is None:
        temp = '-1'
    else:
        temp = data['response']['docs'][i]['type_of_material']
    
    cursor.execute(sql_query_select_id_type_of_material, temp)
    id_type_of_material = cursor.fetchall()
    print(temp, id_type_of_material[0])

    lead_paragraph = ''
        
    if 'lead_paragraph' in data['response']['docs'][i]:
        if data['response']['docs'][i]['lead_paragraph'] is not None:
            lead_paragraph = ''.join([x if x in string.printable else '' for x in data['response']['docs'][i]['lead_paragraph']])
        
    snippet = ''
    if 'snippet' in data['response']['docs'][i]:
        if data['response']['docs'][i]['snippet'] is not None:
            snippet = ''.join([x if x in string.printable else '' for x in data['response']['docs'][i]['snippet']])
        
    title = ''
    if 'main' in data['response']['docs'][i]['headline']:
        title = ''.join([x if x in string.printable else '' for x in data['response']['docs'][i]['headline']['main']])
    
    abstract = ''
    if 'abstract' in data['response']['docs'][i]:
        if data['response']['docs'][i]['abstract'] is not None:
            abstract = ''.join([x if x in string.printable else '' for x in data['response']['docs'][i]['abstract']])  
        
    cursor.execute(sql_query_insert, (id_issue[0], 
                                      data['response']['docs'][i]['web_url'],
                                      snippet, 
                                      lead_paragraph,
                                      title,
                                      data['response']['docs'][i]['word_count'],
                                      id_type_of_material[0],
                                      id_document_type[0],
                                      i,
                                      abstract
                                      )) 
    conn.commit()
print('Running time was ', (time.time()-t)/60)  
conn.commit()
conn.close()
print("MySQL connection closed.")  
