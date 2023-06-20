# -*- coding: utf-8 -*-
"""
Created on Sat Jun 10 10:29:55 2023

@author: Jason
"""

import time
startTime = time.time()
import datetime as dt
import requests
import json
import pandas as pd
import gc



# web scrapping 
# You will need to get your own key jusing your own email here: https://developer.usajobs.gov/API-Reference/
host = 'data.usajobs.gov'  
UserAgent = 'your@email.com'
authKey = 'key data from'
headers = {'Host': host, 'User-Agent': UserAgent, 'Authorization-Key' : authKey}
total_pages = 1
page = 1

days_since_last_ran = 7
today = dt.date.today() -  dt.timedelta(days=days_since_last_ran)
year_ahead = today + dt.timedelta(days=365)

print(f'Pulling all jobs posted since {today}')
while page <= total_pages:
    print(f"Sending request for page {page}")
    try:
        p = requests.get(url=f'https://data.usajobs.gov/api/historicjoa?StartPositionOpenDate={today}T00:00:00&EndPositionOpenDate={year_ahead}T11:59:00&PageNumber={page}', headers=headers)
        print("Request Received")
    except (ConnectionError, ConnectionResetError):
        print('ConnectionError, restarting the loop')
        continue
    json_data = json.loads(p.text)
    total_pages = json_data['paging']['metadata']['totalPages']
    jd = pd.json_normalize(json_data['data'])
    try:
        df = pd.read_parquet('usajobs_today.parquet')
    except FileNotFoundError:
        print('creating the file')
        jd.to_parquet(r'usajobs_today.parquet', index=False) 
        df = pd.read_parquet('usajobs_today.parquet')     
    dfjd = pd.concat([df, jd])
    dfjd.to_parquet(r'usajobs_today.parquet', index=False) 
    del jd
    del df
    del dfjd
    gc.collect()
    percent = round((page/total_pages * 100), 1)
    print(f'Page {page} of {total_pages} ({percent}%) is complete')
    executionTime = (time.time() - startTime)
    print('Execution time in seconds: ' + str(executionTime))
    page +=1
    
print('Done scraping, now merging files')

todaydf = pd.read_parquet('usajobs_today.parquet')


jobs = pd.read_parquet('usajobs_scraped.parquet')
jobs = pd.concat([jobs, todaydf])

del todaydf
gc.collect() 

jobs = jobs.drop_duplicates(subset=['usajobsControlNumber', 'hiringAgencyCode', 'hiringAgencyName',
       'hiringDepartmentCode', 'hiringDepartmentName', 'agencyLevel',
       'agencyLevelSort', 'appointmentType', 'workSchedule', 'payScale',
       'salaryType', 'vendor', 'travelRequirement', 'teleworkEligible',
       'serviceType', 'securityClearanceRequired', 'securityClearance',
       'whoMayApply', 'announcementClosingTypeCode',
       'announcementClosingTypeDescription', 'positionOpenDate',
       'positionCloseDate', 'positionExpireDate', 'announcementNumber',
       'hiringSubelementName', 'positionTitle', 'minimumGrade', 'maximumGrade',
       'promotionPotential', 'minimumSalary', 'maximumSalary',
       'supervisoryStatus', 'drugTestRequired', 'relocationExpensesReimbursed',
       'totalOpenings', 'disableAppyOnline', 'positionOpeningStatus',
       'applicationsStarted'])



jobs.to_parquet(r'usajobs_scraped.parquet', index=False) 


print('accessing current job code data from USAJOBS')

host = 'data.usajobs.gov'  
UserAgent = 'jason.muck@gmail.com'
authKey = 'dZDCAHrYZdqsHJS9CPBo04E3isw7YlHTuwN00so5S/4='
headers = {'Host': host, 'User-Agent': UserAgent, 'Authorization-Key' : authKey}

p = requests.get(url='https://data.usajobs.gov/api/codelist/occupationalseries', headers=headers)
json_data = json.loads(p.text)
a = json_data['CodeList'][0]['ValidValue']
codes = pd.DataFrame(a, dtype = 'str') 
codes = codes[['Code', 'Value']]
codes = codes.rename(columns={"Code": "jobCategories", "Value": "series_title"})

# Data Cleaning starts
# Feature engineering functions

def public_extract(list_dict):
    for i in list_dict:
        if i['hiringPath'] == 'U.S. Citizens, Nationals or those who owe allegiance to the U.S.':
            return 'Y'
            break
        else:
            return 'N'
def compet_fed_extract(list_dict):
    for i in list_dict:
        if i['hiringPath'] == 'Current or former competitive service federal employees.':
            return 'Y'
            break
        else:
            return 'N'
def except_fed_extract(list_dict):
    for i in list_dict:
        if i['hiringPath'] == 'Current excepted service federal employees.':
            return 'Yes'
            break
        else:
            return 'N'
def internal_only_extract(list_dict):
    for i in list_dict:
        if i['hiringPath'] == 'Current federal employees of this agency.':
            return 'Y'
            break
        else:
            return 'N'
    
def str_cleaner(record):
    record = str(record)
    record = record.strip()
    try:
        a = int(record)
        return a
    except:
        if record == 'many':
            return 6
        elif record == 'few':
            return 2
        else:
            return 1
        
def range_maker(record):
    record = str(record)
    record = record.strip()
    try:
        a = int(record)
        if a == 1:
            return '1'
        elif a > 1 and a <= 5:
            return '2 to 5'
        elif a > 5 and a <= 10:
            return '6-10'
        elif a > 10:
            return '> 10'
        
        return a
    except:
        return record

        
def fiscal_year(x):
    month = int(x.strftime("%m"))
    year = int(x.strftime("%Y"))
    if month > 9:
        year += 1
        year = str(year)
        return f'FY {year}'
    else:
        year = str(year)
        return f'FY {year}'

def simple_date(x):
    month = str(x.strftime("%m"))
    year = str(x.strftime("%Y"))    
    date_string = f'{year}-{month}-01'
    date_object = dt.datetime.strptime(date_string, "%Y-%m-%d").date()
    return(date_object)

def get_series(d):
    return str(d['series'])

def ds_finder(job):
    ds_series = ['1515' , '0685', '2210', '1102', '1550', '1520', '0110', '1501', '1560']
    if job in ds_series:
        return 'Y'
    else:
        return 'N'
    
    
print('applying data transformations')
jobs['#_of_hiringPaths'] = jobs['hiringPaths'].apply(len)
jobs['open_to_public'] = jobs['hiringPaths'].apply(public_extract)
jobs['open_to_competitive_feds'] = jobs['hiringPaths'].apply(compet_fed_extract)
jobs['open_to_except_feds'] = jobs['hiringPaths'].apply(compet_fed_extract)
jobs['open_to_agency_only'] = jobs['hiringPaths'].apply(internal_only_extract)
jobs['#_of_job_codes'] = jobs['jobCategories'].apply(len)
jobs['totalOpenings'] = jobs['totalOpenings'].str.lower()
jobs['estimated_totalOpenings'] =  jobs['totalOpenings'].apply(str_cleaner).astype('int')

jobs['positionOpenDate'] = pd.to_datetime(jobs['positionOpenDate'])
jobs['positionCloseDate'] = pd.to_datetime(jobs['positionCloseDate'])
jobs['days_open'] = ((abs(jobs['positionCloseDate'] - jobs['positionOpenDate']))/pd.to_timedelta(1, unit='D')).astype('float')
jobs['fiscal_year'] = jobs['positionOpenDate'].apply(fiscal_year)
jobs['midSalary'] = (jobs['maximumSalary'] + jobs['minimumSalary']) / 2

jobs = jobs.explode('jobCategories').dropna(subset=['jobCategories'])
jobs['jobCategories'] = jobs['jobCategories'].apply(get_series).astype('str')

jobs = jobs.merge(codes, on='jobCategories', how ='left')

jobs['possible_ds'] = jobs['jobCategories'].apply(ds_finder)



jobs.to_parquet(r'usajobs_cleaned.parquet', index=False) 
print('saving data to csv')

jobs = jobs.drop(columns=['hiringPaths', 'positionLocations', 'missionCriticalOccupations', 'keyStandardRequirements', '_links'])
current_date = pd.to_datetime(pd.Timestamp.now().normalize().date())

jobs[(jobs['fiscal_year'] == 'FY 2021') | (jobs['fiscal_year'] == 'FY 2022')].to_csv(r'usajobs_cleaned.csv', index=False)



jobs[jobs['positionCloseDate'] >  current_date ].to_csv(r'usajobs_open.csv', index=False)



executionTime = (time.time() - startTime)
ex_time = executionTime / 60
final_time = round(ex_time, 2)
    
print(f'script complete, total time in minutes is {final_time}')


