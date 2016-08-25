# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import os
import sys
import locale
import xlwt
import codecs
import csv
import pymysql
pymysql.install_as_MySQLdb()
from DrushimJobsCrawler import  settings
import pandas as pd

from twisted.enterprise import adbapi
class DrushimjobscrawlerPipeline(object):


    def __init__(self):

        sys.stdout = codecs.getwriter(locale.getpreferredencoding())(sys.stdout)
        reload(sys)
        sys.setdefaultencoding('utf-8')

        self.book = xlwt.Workbook(encoding='utf-8')
        self.sheet = self.book.add_sheet('Drushim')
        self.sheet.write(0, 0, 'Site')
        self.sheet.write(0, 1, 'Company')
        self.sheet.write(0, 2, 'Company_jobs')
        self.sheet.write(0, 3, 'Job_id')
        self.sheet.write(0, 4, 'Job_title')
        self.sheet.write(0, 5, 'Job_Description')
        self.sheet.write(0, 6, 'Job_Post_Date')
        self.sheet.write(0, 7, 'Job_URL')
        self.sheet.write(0, 8, 'Country_Areas')
        self.sheet.write(0, 9, 'Job_categories')
        self.sheet.write(0, 10, 'AllJobs_Job_class')

        self.last_row = self.sheet.last_used_row

    def close_spider(self, spider):

        unsorted_xls_df = pd.read_excel('unsorted_DrushimJobsList.xls')
        sorted_xls = unsorted_xls_df.sort_values(by='Company')
        sorted_xls.to_excel('DrushimJobsList.xls', sheet_name='Drushim', index=False)
        os.remove('unsorted_DrushimJobsList.xls')


    def process_item(self, item, spider):

        self.last_row += 1
        self.sheet.write(self.last_row, 0, item['DrushimJob']['Site'])
        self.sheet.write(self.last_row, 1, item['DrushimJob']['Company'])
        self.sheet.write(self.last_row, 2, item['DrushimJob']['Company_jobs'])
        self.sheet.write(self.last_row, 3, item['DrushimJob']['Job_id'])
        self.sheet.write(self.last_row, 4, item['DrushimJob']['Job_title'])
        self.sheet.write(self.last_row, 5, item['DrushimJob']['Job_Description'])
        self.sheet.write(self.last_row, 6, item['DrushimJob']['Job_Post_Date'])
        self.sheet.write(self.last_row, 7, item['DrushimJob']['Job_URL'])
        self.sheet.write(self.last_row, 8, item['DrushimJob']['Country_Areas'])
        self.sheet.write(self.last_row, 9, item['DrushimJob']['Job_categories'])
        self.sheet.write(self.last_row, 10, item['DrushimJob']['AllJobs_Job_class'])

        self.book.save('unsorted_DrushimJobsList.xls')

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



