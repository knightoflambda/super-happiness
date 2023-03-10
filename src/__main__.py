__author__ = "Deogracias Aaron P. Dela Cruz"
__copyright__ = "Copyright 2007, The Cogent Project"
__credits__ = ["Rob Knight", "Peter Maxwell", "Gavin Huttley",
 "Matthew Wakefield"]
__license__ = "GPL"
__version__ = "1.0.1"
__maintainer__ = "Rob Knight"
__email__ = "rob@spot.colorado.edu"
__status__ = "Production"


from selenium import webdriver
from selenium.webdriver.common.by import By
from time import sleep
import os
import pandas as pd
import numpy as np
import sqlite3


CHROMEDRIVER_PATH = r'C:\Users\user\Documents\homestead\dolor-sit-amet\data\chromedriver.exe'
DOWNLOAD_DIR = r'C:\Users\user\Documents\homestead\dolor-sit-amet\data\\'
DOWNLOAD_LINK = 'https://jobs.homesteadstudio.co/data-engineer/assessment/download/'


DB_FILE = 'data/datastore.db'
TBL_NAME = 'pivot_table'



def _chrome_webdriver_file_download(
        chrome_driver_path: str,
        download_dir: str,
        download_link: str,
        file_extension_lookup: str
        ) -> str:
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_experimental_option('prefs', {
        'profile.default_content_settings.popups': 0,
        'download.default_directory' : download_dir,
        'directory_upgrade': True
        }
    )

    driver = webdriver.Chrome(chrome_driver_path, chrome_options=chrome_options)
    driver.get(download_link)
    anchor = driver.find_element(By.XPATH, ".//a[contains(text(), 'Download')]")
    anchor.click()

    while not any(f.split('.')[-1] == file_extension_lookup for f in os.listdir(download_dir)):
        sleep(1)
    driver.quit()

    return download_dir + [f for f in os.listdir(download_dir) if f.split('.')[-1] == file_extension_lookup][0]



def _create_pivot_from_data(filename: str) -> pd.DataFrame:
    df = pd.read_excel(filename, sheet_name='data')
    pvt = pd.pivot_table(df, 
                        values=['Spend', 'Attributed Rev (1d)', 
                                'Imprs', 'Visits',
                                'New Visits', 'Transactions (1d)', 'Email Signups (1d)'], 
                        index='Platform (Northbeam)', 
                        aggfunc=np.sum,
                        margins=True,
                        margins_name='Total Result')
    pvt.columns = ['Sum of ' + c for c in pvt.columns]
    pvt = pvt.iloc[:-1,:].sort_values(by='Sum of Attributed Rev (1d)', ascending=False).append(pvt.tail(1)) # can remove appended row if Total Result is not desired
    pvt.reset_index(inplace=True)
    return pvt


def _db_connection(dbfile) -> sqlite3.Connection:
    return sqlite3.connect(dbfile)

def _insert_to_db(cnx: sqlite3.Connection, df: pd.DataFrame, tbl_name: str):
    df.to_sql(tbl_name, cnx, if_exists='replace', index=False)
    cnx.commit()
    cnx.close()

if __name__ == '__main__':
    excel_file = _chrome_webdriver_file_download(CHROMEDRIVER_PATH, DOWNLOAD_DIR, DOWNLOAD_LINK, 'xlsx')
    df_pvt = _create_pivot_from_data(excel_file)
    cnx = _db_connection(DB_FILE)
    _insert_to_db(cnx, df_pvt, TBL_NAME)

