
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

class scrapeJobs():
    def __init__(self, debug=True):
            self.debug = debug 
            self.main_url = 'https://climatebase.org/jobs?l=&q=&categories=Engineering%3A+Software&job_types=Full+time+role&p=1'
            self.table = 'softwarejobs'
    
    def get_jobs(self):
        # selenium set up
        s=Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=s)
        driver.maximize_window()
        driver.get(self.main_url)

        # links
        links = driver.find_elements(By.TAG_NAME,'a')
        links = [x.get_attribute('href') for x in links]
        links = [x for x in links if 'https://climatebase.org/job/' in x]

        # job titles
        titles = driver.find_elements(By.CLASS_NAME,'ListCard__Title-sc-1dtq0w8-2')
        titles = [x.text for x in titles]
        titles = titles[1::3]

        # org, posted, location, tags
        jobs = pd.DataFrame(columns=['organization', 'location', 'posted', 'salary','category'])
        for i in range(1, len(links)+1): 
            org = driver.find_elements(By.XPATH, f'//*[@id="app"]/div[1]/div/div[2]/div[6]/a[{i}]/div/div[2]')[0].text
            location = driver.find_elements(By.XPATH, f'//*[@id="app"]/div[1]/div/div[2]/div[6]/a[{i}]/div/div[3]/div[1]')[0].text
            posted = driver.find_elements(By.XPATH, f'//*[@id="app"]/div[1]/div/div[2]/div[6]/a[{i}]/div/div[3]/div[2]')[0].text
            category = driver.find_elements(By.XPATH, f'//*[@id="app"]/div[1]/div/div[2]/div[6]/a[{i}]/div/div[4]/div')[0].text
            try:
                salary = driver.find_elements(By.XPATH, f'//*[@id="app"]/div[1]/div/div[2]/div[6]/a[1]/div/div[3]/div[2]')[0].text
            except:
                salary = None
            
            job_dict = {'organization': org,
                    'location': location, 
                    'posted': posted, 
                    'salary': salary,
                    'category': category}

            # append to data frame
            jobs = jobs.append(job_dict, ignore_index=True)

        jobs['job_title'] = titles
        jobs['url'] = links
        print('Extracted job metadata.')

        desc_list = []
        for url in links: 
            driver.get(url)
            desc = driver.find_elements(By.XPATH, f'//*[@id="jobPageBody"]/div[8]')[0].text
            desc_list.append(desc)
            time.sleep(1)
        print('Fetched job descriptions.')

        # fill misc columns
        jobs['desc'] = desc_list
        jobs['skills'] = None
        jobs['fetch_date']= pd.to_datetime('today').isoformat()
        self.date = pd.to_datetime('today').strftime("%m_%d_%y")

        # reorder for db write 
        jobs = jobs[['job_title', 'organization', 'location', 'salary', 'url','posted','skills','category', 'desc', 'fetch_date']]

        # source
        jobs['source'] = 'ClimateBase'

        self.jobs =  jobs
        return jobs
    
    def dedupe(self): 
        """dedupe based on last db fetch."""
        jobs = self.jobs

        """Get most recent batch of urls from db to de-dupe."""
        # get database credentials from config file
        with open('ETL/config.json', 'r') as f: 
            self.config = json.load(f)
        config = self.config

        # instantiate connection & cursor
        conn = psycopg2.connect(host="localhost", port = config['port'], database = config['database'], user=config['user'],password=config['password'])
        cur = conn.cursor()

        # define and execute query
        last_batch = f"""SELECT url FROM {self.table} 
        WHERE fetch_date = (SELECT MAX(fetch_date) FROM {self.table} WHERE source = 'ClimateBase')
        AND source = 'ClimateBase'"""
        cur.execute(last_batch)
        query_results = cur.fetchall() 
        query_results = [x[0] for x in query_results]       
        cur.close() # close cursor
        conn.close() # close connection

        # drop any rows where url is already in database
        jobs = jobs[~jobs['url'].isin(query_results)]

        # only continue if there are new jobs
        if len(jobs) > 0:
            print(f'Added {len(jobs)} jobs.')
        else: 
            print('No new jobs to add.')
            quit()
        
        # write to csv
        jobs.to_csv(f'ETL/batches/cb-batch-{self.date}.csv', index=False, sep='\t')

    def write_to_database(self):
        """ Write to PostgreSQL database."""

        # path to file for writing
        data_dir = Path(Path.cwd() / 'ETL' / 'batches' / f'cb-batch-{self.date}.csv')

        # get database credentials from config file
        with open('ETL/config.json', 'r') as f: 
            self.config = json.load(f)
        config = self.config

        # instantiate connection & curson
        conn = psycopg2.connect(host="localhost", port = config['port'], database = config['database'], user=config['user'],password=config['password'])
        cur = conn.cursor()

        # define and execute query
        query = f'''COPY public.{self.table} FROM '{data_dir}' DELIMITER E'\t' CSV HEADER;'''
        cur.execute(query)

        cur.close() # close cursor
        conn.commit() # commit queries
        conn.close() # close connection

        print('Successfully wrote to database.')

    def scrape(self): 
        """Execute scraping & write to db."""
        self.get_jobs()
        self.dedupe()
        self.write_to_database()    


if __name__ == '__main__': 
    scrapeJobs().scrape()