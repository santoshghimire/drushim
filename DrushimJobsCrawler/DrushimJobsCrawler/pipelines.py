# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import os

import codecs
import csv
import pymysql
pymysql.install_as_MySQLdb()
from DrushimJobsCrawler import  settings
import pandas as pd

from twisted.enterprise import adbapi
class DrushimjobscrawlerPipeline(object):

    def __init__(self):

        # self.csvwriter = csv.writer(codecs.open('DrushimJobsList.csv', 'a'))
        self.unsorted_csvwriter = csv.writer(codecs.open('unsorted_DrushimJobsList.csv', 'a'))
        self.unsorted_csvwriter.writerow([
            'Site',
            'Company',
            'Company_jobs',
            'Job_id',
            'Job_title',
            'Job_Description',
            'Job_Post_Date',
            'Job_URL',
            'Country_Areas',
            'Job_categories',
            'AllJobs_Job_class',
        ])

    def close_spider(self, spider):
        df = pd.read_csv('unsorted_DrushimJobsList.csv')
        sorted_csv = df.sort_values(by='Company', ascending=True)
        sorted_csv.to_csv('DrushimJobsList.csv', index=False)
        os.remove('unsorted_DrushimJobsList.csv')




    def process_item(self, item, spider):
        self.unsorted_csvwriter.writerow([
            item['DrushimJob']['Site'],
            item['DrushimJob']['Company'],
            item['DrushimJob']['Company_jobs'],
            item['DrushimJob']['Job_id'],
            item['DrushimJob']['Job_title'],
            item['DrushimJob']['Job_Description'],
            item['DrushimJob']['Job_Post_Date'],
            item['DrushimJob']['Job_URL'],
            item['DrushimJob']['Country_Areas'],
            item['DrushimJob']['Job_categories'],
            item['DrushimJob']['AllJobs_Job_class'],
        ])
        return item


class MySQLPipeline(object):

    def __init__(self, dbpool):
        self.dbpool = dbpool

    @classmethod
    def from_settings(cls,settings):
        dbargs = dict(
            host=settings['MYSQL_HOST'],
            db=settings['MYSQL_DBNAME'],
            user=settings['MYSQL_USER'],
            password=settings['MYSQL_PASSWORD'],
            charset='utf8',
            use_unicode=True,
        )
        dbpool = adbapi.ConnectionPool('MySQLdb', **dbargs)
        return cls(dbpool)

    def process_item(self, item, spider):
        dbpool = self.dbpool.runInteraction(self.insert, item, spider)
        dbpool.addErrback(self.handle_error, item, spider)
        dbpool.addBoth(lambda _: item)
        return dbpool

    def insert(self, conn, item, spider):
        conn.execute("""
            INSERT INTO drushim (
            Site,
            Company,
            Company_jobs,
            Job_id,
            Job_title,
            Job_Description,
            Job_Post_Date,
            Job_URL,
            Country_Areas,
            Job_categories,
            AllJobs_Job_class
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
        """, (
            item['DrushimJob']['Site'],
            item['DrushimJob']['Company'],
            item['DrushimJob']['Company_jobs'],
            item['DrushimJob']['Job_id'],
            item['DrushimJob']['Job_title'],
            item['DrushimJob']['Job_Description'],
            item['DrushimJob']['Job_Post_Date'],
            item['DrushimJob']['Job_URL'],
            item['DrushimJob']['Country_Areas'],
            item['DrushimJob']['Job_categories'],
            item['DrushimJob']['AllJobs_Job_class']

        ))
        spider.log("Item stored in dbSchema: %s %r" % (item['DrushimJob']['Job_id'], item))

    def handle_error(self, failure, item, spider):
        """Handle occurred on dbSchema interaction."""
        self.logger.info("DB Schema Handled")



