import requests
import os
from bs4 import BeautifulSoup
import pandas as pd
import re
import pickle
import datetime 
import yagmail
from cryptography.fernet import Fernet
import traceback
import logging
import bp_sql as bp
import zipfile
import time

script_start_time = time.time()

today = datetime.date.today().strftime("%Y-%m-%d")

#DB path
spp_db = 'SPP.db'

#zip folder path
zip_path = os.path.join(os.getcwd(),  'SPP_Zips')

#contents of zip folder path
zip_list = os.listdir(os.path.join(os.getcwd(),  'SPP_Zips'))

#used for saving files
save_folder = 'SPP_Zips/'

#Website and file type to download
Domain = 'http://mis.ercot.com'
url = 'http://mis.ercot.com/misapp/GetReports.do?reportTypeId=13061&reportTitle=Historical%20RTM%20Load%20Zone%20and%20Hub%20Prices&showHTMLView=&mimicKey'
filetype = 'zip'

mon_dict = {1:'JAN', 2:'FEB', 3:'MAR', 4:'APR', 5:'MAY', 6:'JUN', 7:'JUL', 8:'AUG', 9:'SEP', 10:'OCT', 11:'NOV', 12:'DEC'}
revs_mon_dict = dict([(value, key) for key, value in mon_dict.items()])

def get_credentials():
    key_path = os.path.join(os.path.expanduser('~'), '.fernet')
    key = pickle.load(open(key_path, 'rb'))
    cipher_suite = Fernet(key)
    encrypted_credentials_df = pd.read_csv('encrypted_credentials.csv')

    gmail = 'Gmail'
    gmail_ec_row = encrypted_credentials_df.loc[encrypted_credentials_df.login_account == gmail]
    gmail_user = gmail_ec_row.iloc[0]['username']
    gmail_pwd_encrypt = gmail_ec_row.iloc[0]['encrypted_password']
    gmail_pwd = cipher_suite.decrypt(str.encode(gmail_pwd_encrypt)).decode('utf-8')
    
    return gmail_user, gmail_pwd

def send_email(email_subject, email_contents):
    gmail_user, gmail_pwd = get_credentials()
    
    yag = yagmail.SMTP(user=gmail_user, password=gmail_pwd)
    
    yag.send(to=gmail_user, subject=email_subject, contents=email_contents)

def check_max_date(db_file):
    conn = bp.create_connection(db_file)
    max_date_df = pd.read_sql_query('''Select max(DELIVERY_DATE) as MAX_DELIVERY_DATE,  max(DELIVERY_HOUR) as MAX_DELIVERY_HOUR 
                                       from ercot_hist_spp 
                                       where delivery_date = (select max(delivery_date) from ercot_hist_spp where settlement_point_price is not null)''', conn)
    
    max_date =  pd.to_datetime(max_date_df['MAX_DELIVERY_DATE'][0])
    max_hour = max_date_df['MAX_DELIVERY_HOUR'][0]
    conn.close()
    return max_date, max_hour

def get_sheet_list(file:str, year:int):
    with zipfile.ZipFile(file) as zipped_file:
        summary = zipped_file.open(r'xl/workbook.xml').read()
    soup = BeautifulSoup(summary, 'xml')
    sheet_list = [sheet.get("name") for sheet in soup.find_all("sheet")]

    if year == min_file_year:
        sheets_list = [x for x in sheet_list if revs_mon_dict.get(str(x[0:3]).upper())>= min_file_mon]

    return sheet_list 
    


sql_create_spp_table = ''' Create Table if not exists ercot_hist_spp (
                                    DELIVERY_DATE text,
                                    DELIVERY_HOUR integer,
                                    DELIVERY_INTERVAL integer,
                                    REPEATED_HOUR_FLAG text,
                                    SETTLEMENT_POINT_NAME text,
                                    SETTLEMENT_POINT_TYPE text,
                                    SETTLEMENT_POINT_PRICE real);
                                '''

sql_create_spp_tbl_index = '''Create index IF NOT EXISTS index_dd_ercot_hist_spp on ercot_hist_spp (DELIVERY_DATE)'''

sql_create_spp_view = ''' Create View if not exists ercot_avg_spp as
                                Select DELIVERY_DATE, DELIVERY_HOUR, SETTLEMENT_POINT_NAME, SETTLEMENT_POINT_TYPE, AVG(SETTLEMENT_POINT_PRICE) as SETTLEMENT_POINT_PRICE 
                                from ercot_hist_spp 
                                group by DELIVERY_DATE, DELIVERY_HOUR, SETTLEMENT_POINT_NAME, SETTLEMENT_POINT_TYPE
                                ;
                                '''


#create tbl and view if they dont exist
create_list = [sql_create_spp_table, sql_create_spp_tbl_index, sql_create_spp_view]
for c in create_list:
    bp.create_table(spp_db, c)



try:
    #get max date of data in db
    max_date, max_hour = check_max_date(spp_db)
    
    if None not in (max_date, max_hour):
        max_year = max_date.year
        max_mon = max_date.month
        max_day = max_date.day
    else:
        max_year, max_mon, max_day = 2000, 1 , 1
        max_date = date(max_year, max_mon, max_day).strftime("%m/%d/%y")
        

    max_mon_abrv = mon_dict.get(max_mon)

    if max_mon == 12 and max_day == 31 and max_hour == 24:
        min_file_year = max_year + 1
        min_file_mon = 1
    else:
        min_file_year = max_year
        min_file_mon = max_mon


    #Get websites HTML, get all the filename and associated links
    soup = BeautifulSoup(requests.get(url).text, 'html.parser')
    file_list = soup.find_all(class_='labelOptional_ind')
    link_list = soup.findAll('a', attrs={'href': re.compile("/misdownload/")}) 


    #Slim down the information in the previous list and put them into new lists

    link_name_list = []

    for link in link_list:
        link_name_list.append(link.get('href'))

    folder_name_list = []    

    for class_ in file_list:
        folder_name_list.append(str(class_.next_element))




    #create a df of filename and links 
    data_dict = {'ZipFolderName':folder_name_list, 'DownLoadLink':link_name_list}
    dl_zip_df = pd.DataFrame(data_dict)
    dl_zip_df['ZipFolderYear'] = dl_zip_df.ZipFolderName.str.split('.').str[5].str[-4:]
    dl_zip_df['ZipFolderYear'] = dl_zip_df['ZipFolderYear'].astype(int)
    dl_zip_df['ZipFolderDate'] = dl_zip_df.ZipFolderName.str.split('.').str[3]
    dl_zip_df['ZipFolderDate'] = dl_zip_df['ZipFolderDate'].astype(int)

    zip_list_df = []
    zip_list_df = pd.DataFrame(zip_list, columns=['filename'])
    zip_list_df['filetype'] = zip_list_df.filename.str.split('.').str[-1]
    zip_list_df = zip_list_df[zip_list_df['filetype']=='zip']
    zip_list_df['filedate'] = zip_list_df.filename.str.split('.').str[3]
    zip_list_df['fileyear'] = zip_list_df.filename.str.split('.').str[5].str[-4:]
    zip_list_df['fileyear'] = zip_list_df['fileyear'].astype(int)
    zip_list_df['filedate'] = zip_list_df['filedate'].astype(int)

    join_df = zip_list_df.merge(dl_zip_df, how='outer', left_on='filedate', right_on='ZipFolderDate')
    join_df.reset_index(drop=True, inplace=True)
    join_df.fillna(int(0), inplace=True)
    new_max_file_date = max(join_df['ZipFolderDate'])


    #dl_zip_df Download each zip from df that hasn't been downloaded already and save to specific folder in wd
    for i in range(len(join_df)):
        #download new zip and delete any old zips
        if (join_df['ZipFolderDate'][i]>join_df['filedate'][i]) and (join_df['ZipFolderYear'][i]>= min_file_year):
            with open(save_folder + join_df['ZipFolderName'][i], 'wb') as file:
                response = requests.get(Domain + join_df['DownLoadLink'][i])
                file.write(response.content)

        elif 0 < join_df['filedate'][i] < new_max_file_date:
            os.remove(os.path.join(os.getcwd(),  'SPP_Zips', join_df['filename'][i]))


    #extracts the xlsx from each zip and places in same directory
    for file in [j for j in os.listdir(zip_path) if '.zip' in j]:
            with ZipFile(zip_path + '/' + file, 'r') as zipObj:
                zipObj.extractall(path=zip_path)



    bp.delete_rows(spp_db, delete_sql='''delete from ercot_hist_spp where settlement_point_price is null''')

    #create blank lists
    file_list, month_list, start_date_list, end_date_list, time_list  = ([] for i in range(5))

    column_dict = {'Delivery Date':'DELIVERY_DATE'
                   , 'Delivery Hour':'DELIVERY_HOUR'
                   ,'Delivery Interval':'DELIVERY_INTERVAL'
                   ,'Repeated Hour Flag':'REPEATED_HOUR_FLAG'
                   ,'Settlement Point Name':'SETTLEMENT_POINT_NAME'
                   ,'Settlement Point Type':'SETTLEMENT_POINT_TYPE'
                   ,'Settlement Point Price':'SETTLEMENT_POINT_PRICE'}



    conn = bp.create_connection(spp_db)

    get_new_data_start_time = time.time()
     
    for file in [f for f  in sorted(os.listdir(os.path.join(os.getcwd(),  'SPP_Zips'))) if '.xlsx' in f]:

        file_year = int(file[-9:-5])

        if  file_year >= min_file_year:
            file_path = os.path.join(os.getcwd(),  'SPP_Zips', file)
            sheet_list = get_sheet_list(file_path, file_year)
            upload_df = pd.concat(pd.read_excel(file_path, sheet_name=sheet_list), ignore_index=True)
            upload_df.dropna(inplace=True)
            upload_df['Delivery Date'] = pd.to_datetime(upload_df['Delivery Date'])
            upload_df = upload_df[upload_df['Delivery Date']>max_date]

            if len(upload_df)>0:
                min_del_date = min(upload_df['Delivery Date'])
                max_del_date = max(upload_df['Delivery Date'])
                upload_df.rename(columns=column_dict, inplace=True)
                upload_df.to_sql(name='ercot_hist_spp', con=conn, if_exists='append', index=False)

                file_list.append(file)
                start_date_list.append(min_del_date)
                end_date_list.append(max_del_date)
                time_list.append(time.time() - get_new_data_start_time)

    bp.vacuum_db(spp_db)
    conn.close()
    
    email_dict = {'file':file_list, 'start_date': start_date_list, 'end_date':end_date_list, 'loop_duration': time_list}
    email_df = pd.DataFrame(email_dict)
    

    
except Exception as e:
    send_email(email_subject = 'ERCOT SPP Scrape - ' + today + ' - FAILED', email_contents='An error occured during the Eroct SPP Scraping Process: /n' + logging.error(traceback.format_exc()))
    
else:
    total_script_time = time.time() - script_start_time
    total_script_time = str(datetime.timedelta(seconds=total_script_time))
    print(total_script_time)
    send_email(email_subject = 'ERCOT SPP Scrape - ' + today, email_contents=['Script Time (Secs): '+ str(total_script_time) + '\n\n', email_df])
