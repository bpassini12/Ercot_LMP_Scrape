# %%
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
#import html5lib

#for chrome driver
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
options = Options()
options.headless = True
from pyvirtualdisplay import Display
display = Display(visible=0, size=(800, 800))  
display.start()


# %% [markdown]
# ## Set Up dates and folders

# %%
script_start_time = time.time()

today = datetime.date.today().strftime("%Y-%m-%d")

#DB path
spp_db = 'SPP.db'

zip_fldr = 'SPP_Zips'

cwd = os.getcwd()

#zip folder path
zip_fldr_path = os.path.join(cwd,  zip_fldr)

#contents of zip folder path
zip_list = os.listdir(os.path.join(cwd, zip_fldr))

#some kind of chromdriver option idk if necessary
prefs = {"download.default_directory" : zip_fldr_path}
options.add_experimental_option("prefs",prefs)

#Website and file type to download
Domain = 'http://ercot.com'
url = 'https://www.ercot.com/mp/data-products/data-product-details?id=NP6-785-ER'

mon_dict = {1:'JAN', 2:'FEB', 3:'MAR', 4:'APR', 5:'MAY', 6:'JUN', 7:'JUL', 8:'AUG', 9:'SEP', 10:'OCT', 11:'NOV', 12:'DEC'}
revs_mon_dict = dict([(value, key) for key, value in mon_dict.items()])

# %% [markdown]
# ## Define Functions

# %%
def get_credentials():
    '''read in gmail creds'''
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
    '''send email using gmail'''
    gmail_user, gmail_pwd = get_credentials()
    
    yag = yagmail.SMTP(user=gmail_user, password=gmail_pwd)
    
    yag.send(to=gmail_user, subject=email_subject, contents=email_contents)

def check_max_date(db_file):
    '''check the spp db for the last date and record that was saved.
       This will be used for a starting point when looking to pull new data'''

    conn = bp.create_connection(db_file)
    max_date_df = pd.read_sql_query('''Select max(DELIVERY_DATE) as MAX_DELIVERY_DATE,  max(DELIVERY_HOUR) as MAX_DELIVERY_HOUR 
                                       from ercot_hist_spp 
                                       where delivery_date = (select max(delivery_date) from ercot_hist_spp where settlement_point_price is not null)''', conn)
    
    max_date =  pd.to_datetime(max_date_df['MAX_DELIVERY_DATE'][0])
    max_hour = max_date_df['MAX_DELIVERY_HOUR'][0]
    conn.close()
    return max_date, max_hour

def get_sheet_list(file:str, year:int):
    '''for each xlsx that needs to be looked at, get the list of sheets that need to be read in and compared
       If reading in the same years xlsx as the max date from db, only get sheets as of that max date and later'''

    with zipfile.ZipFile(file) as zipped_file:
        summary = zipped_file.open(r'xl/workbook.xml').read()
    soup = BeautifulSoup(summary, 'xml')
    sheet_list = [sheet.get("name") for sheet in soup.find_all("sheet")]

    if year == min_file_year:
        sheets_list = [x for x in sheet_list if revs_mon_dict.get(str(x[0:3]).upper())>= min_file_mon]

    return sheet_list 
    

# %% [markdown]
# ## Define SQL Strings 
# - create view, tbl, and index if it does not exist

# %%
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

# %% [markdown]
# ## Run the Scraping Process

# %%
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
    driver = webdriver.Chrome(chrome_options=options)
    driver.get(url)

    html = driver.page_source
    soup = BeautifulSoup(html, 'xml')

    file_list = soup.find_all(class_='name')
    friendly_list = [f.next_element for f in file_list]
    long_name_list = [f['title'] for f in file_list]
    link_list = soup.findAll('a', attrs={'href': re.compile("/misdownload/")}) 
    link_list = [f['href'] for f in link_list]

    website_file_df = pd.DataFrame(zip(friendly_list, long_name_list, link_list), columns=['web_friendly_name','web_long_name','web_link'])  


    website_file_df['web_file_yr'] = website_file_df.web_friendly_name.apply(lambda x: int(x[-4:]))
    website_file_df['web_file_date'] = website_file_df.web_long_name.apply(lambda x: pd.to_datetime(x.split('.')[3]))


    zip_list_df = pd.DataFrame(zip_list, columns=['fldr_filename'])
    zip_list_df['fdr_friendly_name'] = zip_list_df.fldr_filename.apply(lambda x: x.split('.')[3])

    merge_df = website_file_df.merge(zip_list_df,how='outer',left_on='web_friendly_name', right_on='fdr_friendly_name')



    #Download necessary zips
    for i, r in merge_df[~merge_df.web_long_name.isna()].iterrows():

        #download new zip
        if (r['web_file_date']>max_date and r['web_file_yr']>= min_file_year):
            with open(os.path.join(zip_fldr_path, r['web_long_name']), 'wb') as file:
                response = requests.get(r['web_link'])
                file.write(response.content)

    driver.quit()

    #extracts the xlsx from each zip and places in same directory
    z_list = [j for j in os.listdir(zip_fldr_path) if '.zip' in j]

    #removes zips after extracting excel
    for file in z_list:
        with zipfile.ZipFile(zip_fldr_path + '/' + file, 'r') as zipObj:
            zipObj.extractall(path=zip_fldr_path)
            os.remove(os.path.join(zip_fldr_path, file))

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
        
    for file in [f for f  in sorted(os.listdir(zip_fldr_path)) if '.xlsx' in f]:

        file_year = int(file[-9:-5])

        if  file_year >= min_file_year:
            file_path = os.path.join(zip_fldr_path, file)
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


