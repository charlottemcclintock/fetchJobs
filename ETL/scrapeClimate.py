
#%%
import requests # APIs
from bs4 import BeautifulSoup # web scraping
import pandas as pd
import time
import psycopg2
from pathlib import Path
import json
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By

# selenium set up
s=Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=s)
driver.maximize_window()
driver.get('https://climatebase.org/jobs?l=&q=&categories=Data+Scientist&job_types=Full+time+role&p=1')

# links
links = driver.find_elements(By.TAG_NAME,'a')
links = [x.get_attribute('href') for x in links]
links = [x for x in links if 'https://climatebase.org/job/' in x]

# title, org, posted, location, tags
jobs = pd.DataFrame(columns=['job_title', 'organization', 'location', 'posted', 'category'])
for i in range(1, len(links)+1): 
    title = driver.find_elements(By.XPATH, f'//*[@id="app"]/div[1]/div/div[2]/div[6]/a[{i}]/div[2]/div[1]/div[2]/div')[0].text
    org = driver.find_elements(By.XPATH, f'//*[@id="app"]/div[1]/div/div[2]/div[6]/a[{i}]/div[2]/div[2]')[0].text
    location = driver.find_elements(By.XPATH, f'//*[@id="app"]/div[1]/div/div[2]/div[6]/a[{i}]/div[2]/div[3]/div[1]')[0].text
    posted = driver.find_elements(By.XPATH, f'//*[@id="app"]/div[1]/div/div[2]/div[6]/a[{i}]/div[2]/div[3]/div[2]')[0].text
    category = driver.find_elements(By.XPATH, f'//*[@id="app"]/div[1]/div/div[2]/div[6]/a[{i}]/div[2]/div[4]/div')[0].text

    job_dict = {'job_title': title,
            'organization': org,
            'location': location, 
            'posted': posted, 
            'category': category}

    # append to data frame
    jobs = jobs.append(job_dict, ignore_index=True)

jobs['url'] = links

desc_list = []
for url in links: 
    driver.get(url)
    desc = driver.find_elements(By.XPATH, f'//*[@id="jobPageBody"]/div[8]')[0].text
    desc_list.append(desc)
    time.sleep(1)

jobs['desc'] = desc_list
jobs['skills'] = None
jobs['salary'] = None
jobs['fetch_date']= pd.to_datetime('today').isoformat()

jobs = jobs[['job_title', 'organization', 'location', 'salary', 'url','posted','skills','category', 'desc', 'fetch_date']]
# %%
