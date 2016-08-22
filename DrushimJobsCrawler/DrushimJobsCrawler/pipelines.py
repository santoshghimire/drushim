# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import codecs
import sys
import locale
import codecs
import csv
class DrushimjobscrawlerPipeline(object):

    def __init__(self):

        self.csvwriter = csv.writer(codecs.open('DrushimJobsList.csv', 'a'))


        self.csvwriter.writerow([
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

    def process_item(self, item, spider):
        self.csvwriter.writerow([
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
