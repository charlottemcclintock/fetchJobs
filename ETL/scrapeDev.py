
"""
Scrape jobs from Tech Jobs for Good jobs board & write to database.
"""

import requests # APIs
from bs4 import BeautifulSoup # web scraping
import pandas as pd
import time
import psycopg2
from pathlib import Path
import json

class scrapeJobs():
    def __init__(self, debug=True):
            self.debug = debug 
    
    def get_metadata(self):
        """Get job metadata from box layouts."""

        # homepage url
        url = 'https://techjobsforgood.com/jobs/?q=&job_function=Data+%2B+Analytics&sort_by=date&page=1'

        # get page content
        page = requests.get(url)
        soup = BeautifulSoup(page.content, "html.parser")

        # get all cards to iterate
        cards = soup.findAll('div', {'class':'content', 'style': 'cursor: pointer;'})

        # create data frame of jobs
        jobs = pd.DataFrame(columns=['job_title', 'organization', 'location', 'salary'])

        for job in cards:
            # get title, organization, location, salary (if exists), and url
            title = job.find('div', {'class': 'header'}).get_text(strip=True)
            org = job.find('span', {'class':'company_name'}).get_text(strip=True)
            location = job.find('span', {'class':'location'}).get_text(strip=True)
            try:   
                salary = job.find('span', {'class':'salary'}).get_text(strip=True)
            except: 
                salary = None
            url = 'https://techjobsforgood.com' + job.get('onclick')[15:-2]
            # create dict
            job_dict = {'job_title': title,
            'organization': org,
            'location': location, 
            'salary': salary, 
            'url': url}
            # append to data frame
            jobs = jobs.append(job_dict, ignore_index=True)


        self.jobs = jobs
        print('Extracted job metadata.')
        return jobs
    
    def get_description(self):
        """Get detailed job information from each job page."""

        # create data frame of job descriptions
        desc = pd.DataFrame(columns=['url', 'posted', 'skills', 'category', 'desc'])

        for url in self.jobs['url']: 
            # get html
            page = requests.get(url)
            soup = BeautifulSoup(page.content, "html.parser")
            # get date posted, skills, description, and category (if exists)
            posted = soup.find('span', {'class':'f6', 'itemprop':'datePosted'}).get_text(strip=True)
            skills = soup.findAll('div', {'class':'ui basic blue label'})
            skill_list = []
            for skill in skills: 
                skill_list.append(skill.get_text())
            skill_list = ", ".join(skill_list)
            description = soup.find('div', {'itemprop':'description'}).get_text(strip=True)
            try:  
                category = soup.find('div', {'class':'ui label', 'style':'background-color: #0047BB;color: #F1F1F1;'}).get_text(strip=True)
            except:
                category = 'None'
            # create dict
            desc_dict = {'url': url, 
            'posted': posted, 
            'skills': skill_list, 
            'category': category, 
            'desc': description}
            # append to df
            desc = desc.append(desc_dict, ignore_index=True)
            # sleep to avoid maxing out requests
            time.sleep(0.25)

        print('Fetched job descriptions.')
        return desc

    def join_data(self): 
        """Join & write out data."""
        jobs =  self.get_metadata()
        desc = self.get_description()

        # merge descriptions to jobs on url
        jobs = jobs.merge(desc, on='url')

        """Get most recent batch of urls from db to de-dupe."""
        # get database credentials from config file
        with open('ETL/config.json', 'r') as f: 
            self.config = json.load(f)
        config = self.config
        # instantiate connection & cursor
        conn = psycopg2.connect(host="localhost", port = config['port'], database = config['database'], user=config['user'],password=config['password'])
        cur = conn.cursor()
        # define and execute query
        last_batch = """SELECT url FROM datajobs 
        WHERE fetch_date = (SELECT MAX(fetch_date) FROM datajobs WHERE source = 'Tech Jobs for Good')
        AND source = 'Tech Jobs for Good'"""
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

        # add column for fetch date
        self.date = pd.to_datetime('today').strftime("%m_%d_%y")
        jobs['fetch_date']= pd.to_datetime('today').isoformat()

        # source
        jobs['source'] = 'Tech Jobs for Good'

        # write to csv
        jobs.to_csv(f'ETL/batches/tj4g-batch-{self.date}.csv', index=False, sep='\t')
    
    def write_to_database(self):
        """ Write to PostgreSQL database."""

        # path to file for writing
        data_dir = Path(Path.cwd() / 'ETL' / 'batches' / f'tj4g-batch-{self.date}.csv')
        config = self.config 

        # instantiate connection & curson
        conn = psycopg2.connect(host="localhost", port = config['port'], database = config['database'], user=config['user'],password=config['password'])
        cur = conn.cursor()

        # define and execute query
        query = f'''COPY public.datajobs FROM '{data_dir}' DELIMITER E'\t' CSV HEADER;'''
        cur.execute(query)

        cur.close() # close cursor
        conn.commit() # commit queries
        conn.close() # close connection

        print('Successfully wrote to database.')

    def scrape(self): 
        """Execute scraping & write to db."""
        self.join_data()
        self.write_to_database()    


if __name__ == '__main__': 
    scrapeJobs().scrape()
