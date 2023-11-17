## Objective: 
'''
1. ETL on OCO2 GEOS Level 3 data
2. Store on S3 Bucket
3. Create visualization/animation

'''
###################################################
## Airflow operators
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operator.s3 import S3CopyObjectOperator, S3Hook
from airflow import DAG
from datetime import datetime, timedelta


###################################################
## libraries for data access and pre-processing
## preprocessing: retireve data from netCDF and convert to pandas columns
import netCDF4 # packages to open 'netcdf' file
import numpy as np # numpy and pandas packages to pre-process the dataset
import pandas as pd

# for visualization
import matplotlib.pyplot as plt # to create plots and graphs
from mpl_toolkits.basemap import Basemap # to create geo-spatial map, requires dependencies installation
import plotly.express as px
import matplotlib.patches as mpatches
import matplotlib as mpl
import matplotlib.animation as animation
from matplotlib.collections import PatchCollection
import time
import matplotlib

# Data ACCESS from OPENDAD source
## Libraries
from urllib import request, parse
import getpass
import netrc
import os
import requests
import matplotlib.image as mpimg
import os
import time
from netCDF4 import Dataset

# pydap library to open session
from pydap import client
from pydap.cas.urs import setup_session
from pydap.client import open_url
import json

# to load webcontent and retrieve data from link
from IPython.display import display, HTML
from IPython import display
from datetime import datetime

## Loading Screen
from tqdm import tqdm
import time

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# to grab data from entire year, month
from bs4 import BeautifulSoup #
from IPython.display import Image


########################################
########################################

## Default Args;
'''
- Set the schedule to start every two-weeks
- OCO2 data collected every 15 day cycle period
'''

default_args= {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.now(),
    "retries": 1,
    "retry_delay": timedelta(minutes= 5),

}

dag= DAG(
    dag_id="oco2_etl_workflow",
    default_args= default_args,
    schedule_interval= None,
    description="Schedule Airflow work every two weeks",
    schedule_interval= timedelta(weeks= 2),
)

## Python operator
### function: to perform data-extraction from OpenDAP server
def extract_data(year: int ,month: int, username: str, password: str):


    """
    ## Get Session
    - To get Access openDAP server; we create session TOKEN and make requests to get data
    ### Future work:
    - create asynchronous functions for making multiple requests without waiting time
    """
    def get_session(url, file_name):
        """
        Creating a session with url and filename in openDap for data retrieval
        https://oco2.gesdisc.eosdis.nasa.gov/opendap/
        """
        try:
            login_credentials= 'uat.urs.earthdata.nasa.gov'
            username, _, password = netrc.netrc().authenticators(login_credentials)
        except (FileNotFoundError, TypeError):
            # FileNotFound = There's no .netrc file
            # TypeError = The endpoint isn't in the netrc file, causing the above to try unpacking None
            print('\n*******************************************\n')
            print('Please provide your Earthdata Login credentials to allow data access\n')
            print('Your credentials will only be passed to %s and will not be exposed in Jupyter' % (url))
            print('\n')
            # username = input('Username: ')

            username = username
            password= password
            #password = getpass.getpass()
            print('\n*******************************************\n')

        # pydap session
        session = setup_session(username, password, check_url= url + file_name)

        # using the session to get access the data
        return session
    

    ## Pre-process the Extracted data
    ## INPUT for specified year, version of file
    # year= input("Enter the Year: ")
    # month= input("Enter the Month: ")
    # ver_= input("Enter the version: ")

    year= year
    month= month

    ## FORMAT the filenames by VERSION and YEAR

    ### OCO2 GEOS level 3 file
    lite_file= '/OCO2_GEOS_L3CO2_DAY.10r/' + year

    # ########################################################################
    # # GET THE files from OpenDap source. Open the netcdf file and store in dataframe
    # # Beautiful soup to retrieve all data for the MONTH
    # # EXAMPLE: in this test, we will use SINGLE file of the month
    # ########################################################################
    # 
    main_url='https://oco2.gesdisc.eosdis.nasa.gov/opendap'
    content= '/contents.html'

    s= requests.get(main_url + lite_file + content)

    ## Scrap the content by specified URL
    ## Get the entire filenames of the searched YEAR
    soup= BeautifulSoup(s.content, 'html.parser')

    list_files=[]
    ## regex expression to GET netCDF files
    html_links= soup.select('a[href$=".nc4.html"]')

    for link in html_links:
        list_files.append(link['href'])
    # 
    # ## pre-process the filenames; strings, CLEAN the files
    # # removing last strings '.html' to download the files from PYDAP library to match file names
    files_oco2= [f[:-5] for f in list_files]
    # 
    # # total_files= ['opendap'+lite_file+'/'+ f for f in files_oco2[:3]]
    total_files= [lite_file+'/'+ f for f in files_oco2]
    # 
    # ## get alternate row files; duplicate ROWS on html LINKS
    total_files= total_files[::2]

    """### Example: Retrieving the file for OCO3 from `openDAP` source
    - https://oco2.gesdisc.eosdis.nasa.gov/opendap
    - /OCO3_L2_Lite_FP.10.4r
    - year: /2020
    - Month: 01
    - Day: 06
    - /oco3_LtCO2_200106_B10400Br_220317235330s.nc4.html
    """

    """# 6. GET session TOKEN using the EarthData website credentials
    - using the same `session` token to request multiple files
    """

    url=main_url
    session= get_session(url, total_files[0])

    """* Total files for the Entire Year"""

    len(total_files)

    """## 6.a. Get files for Search/Query: year and month
    ### File format on OpenDAP server: `'/OCO3_L2_Lite_FP.10.4r/2020/oco3_LtCO2_20'`
    - Filter the files to get specific month files
    """

    ### RE-FORMAT the FILENAME; performing substrings
    total_= [f for f in total_files if f.startswith( total_files[0][:54] + str(month)) ]

    """### Example: 15 Day cycle period
    - Ploting 15 day cycle period for the single month searched
    """
    ### TOTAL 15 day cycle
    days_= 30
    total_= total_[:days_]

    print("Total files: ")
    print(total_[:5])

    """## Following function;
    1. Performs iterative process to access and retrieve multiple files
    2. Using the same `SESSION TOKEN` to retrieve data from `OPENDAP` website

    ## Data Access to Multiple files
    - takes in pydap data and appends all the variables together
    """
    ## mkdir file
    if not os.path.exists("oco2_jpeg"):
        os.mkdir("oco2_jpeg/")


    m= Basemap()
    m.drawcoastlines(linewidth=0.40)
    m.colorbar(label="XCO2", location= "bottom",format= "%.2e")

    if session:
    #     print("alive")

    ## progress bar
        #total_iterations= len(total_)
        #progress_bar= tqdm( total= total_iterations, unit= "MB")

        for j in range(0, len(total_)):
            pydap_df= open_url(main_url + total_[j], session=session)
            # print(pydap_df.attributes.keys)

            ### Create matplotlib visualization plots
            xco2= pydap_df["XCO2"][:]

            year_, month_, day_= total_[j][50:-17], total_[j][54:-15], total_[j][56:-13]  

            m.imshow(xco2.data[0], )#cmap="magma")

            plt.title("OCO2-GEOS_L3_CO2_Day\n"+ year_ +"-"+month_+"-"+day_)


            # cbar= plt.colorbar(label="XCO2", orientation="horizontal", format='%.2e')
            # cbar.ax.tick_params(labelsize=7.5) 

            # m.colorbar(label="XCO2", location= "bottom",format= "%.2e")
            plt.gcf().set_size_inches(8, 6)
            plt.savefig("oco2_jpeg/" + str(j) + "_.jpeg", dpi= 200)

    else:
        print("request new session")


## python operator
extract_data_= PythonOperator(
    task_id= "extract_oco2_data",
    python_callabel= extract_data,
    op_kwargs={
        'year': 2021,
        'month': 10,
        'username': os.environ.get("earthdata_username"),
        'password': os.environ.get("earthdata_password"),

    },
    dag= dag,
)

extract_data_
