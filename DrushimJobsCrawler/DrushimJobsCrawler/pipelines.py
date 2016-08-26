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
import pymysql
import shutil
import pandas as pd
from xlutils.copy import copy
from xlrd import open_workbook
from twisted.enterprise import adbapi

pymysql.install_as_MySQLdb()

from DrushimJobsCrawler import  settings

excel_file_path = "../../site_data.xls"


class DrushimjobscrawlerPipeline(object):

    def __init__(self):

        self.sheet_name = 'Drushim'  # name of the sheet for current website
        self.unsorted_temp_site_data_xls = 'unsorted_site_data.xls'  # temporary xls file which contain scraped item
        sys.stdout = codecs.getwriter(locale.getpreferredencoding())(sys.stdout)
        reload(sys)
        sys.setdefaultencoding('utf-8')

        if os.path.isfile(excel_file_path):
            """ check if site_data.xls exists we will copy old xls and append to last row"""
            self.file_exists = True
            self.read_old_book = open_workbook(excel_file_path)
            self.clone_old_book = copy(self.read_old_book)
            try:
                """ if  the self.sheet_name for the site exists in site_data.xls exists"""
                self.sheet_index = self.read_old_book.sheet_names().index(self.sheet_name)
                self.clone_old_ws = self.clone_old_book.get_sheet(self.sheet_index)
                self.next_row = self.clone_old_ws.last_used_row
                self.sheet = self.clone_old_ws
            except:
                """ the self.sheet_name  for the site not exists in site_data.xls exists we will create the sheet"""
                self.sheet = self.clone_old_book.add_sheet(self.sheet_name)
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

                self.next_row = self.sheet.last_used_row

            self.book = self.clone_old_book


        else:
            """ check if site_data.xls exists we will create the excel file and add self.sheet_name of the website"""
            self.file_exists = False
            self.book = xlwt.Workbook(encoding='utf-8')
            self.sheet = self.book.add_sheet(self.sheet_name)
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

            self.next_row = self.sheet.last_used_row

    def close_spider(self, spider):

        """ if site_data.xls exists"""
        if self.file_exists:
            try:
                """ Will remove old excel_file_path and
                save new file which is sorted version of unsorted_temp_site_data_xls"""

                os.remove(excel_file_path)
                unsorted_xls = open_workbook(self.unsorted_temp_site_data_xls, on_demand=True)
                sheet_name_list = unsorted_xls.sheet_names()
                writer = pd.ExcelWriter(excel_file_path)
                # writer = pd.ExcelWriter()
                for sheet_name in sheet_name_list:
                    unsorted_xls_df = pd.read_excel(self.unsorted_temp_site_data_xls, sheetname=sheet_name)
                    sorted_xls = unsorted_xls_df.sort_values(by='Company')
                    sorted_xls = sorted_xls.drop_duplicates() # remove duplicates
                    sorted_xls.to_excel(writer, sheet_name=sheet_name, index=False)
                writer.save()
                os.remove(self.unsorted_temp_site_data_xls)
            except:
                """ For any reason the pd.ExcelWriter cannot write and save the file
                    We will just move the unsorted_tem_site_data_xls to excel_file_path"""

                if not os.path.isfile(excel_file_path):
                    shutil.move(self.unsorted_temp_site_data_xls, excel_file_path)

        else:
            """ if site_data.xls doesnot exists"""
            unsorted_xls_df = pd.read_excel(self.unsorted_temp_site_data_xls)
            sorted_xls = unsorted_xls_df.sort_values(by='Company')
            sorted_xls = sorted_xls.drop_duplicates()
            sorted_xls.to_excel(excel_file_path, index=False, sheet_name=self.sheet_name)
            os.remove(self.unsorted_temp_site_data_xls)

    def process_item(self, item, spider):

        self.next_row += 1
        self.sheet.write(self.next_row, 0, item['DrushimJob']['Site'])
        self.sheet.write(self.next_row, 1, item['DrushimJob']['Company'])
        self.sheet.write(self.next_row, 2, item['DrushimJob']['Company_jobs'])
        self.sheet.write(self.next_row, 3, item['DrushimJob']['Job_id'])
        self.sheet.write(self.next_row, 4, item['DrushimJob']['Job_title'])
        self.sheet.write(self.next_row, 5, item['DrushimJob']['Job_Description'])
        self.sheet.write(self.next_row, 6, item['DrushimJob']['Job_Post_Date'])
        self.sheet.write(self.next_row, 7, item['DrushimJob']['Job_URL'])
        self.sheet.write(self.next_row, 8, item['DrushimJob']['Country_Areas'])
        self.sheet.write(self.next_row, 9, item['DrushimJob']['Job_categories'])
        self.sheet.write(self.next_row, 10, item['DrushimJob']['AllJobs_Job_class'])

        self.book.save(self.unsorted_temp_site_data_xls)

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



