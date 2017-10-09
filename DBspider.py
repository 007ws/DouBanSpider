#!/usr/bin/evn python
# -*- coding: utf-8 -*-

# E-mail  :wangs_sdlc@163.com
# Ctime   :2017/09/20

import urllib2
import time
import random
import sys
import threading
import MySQLdb
from bs4 import BeautifulSoup
from openpyxl import Workbook
from Queue import Queue

import resource
from spider_log import Logger

reload(sys)
sys.setdefaultencoding('utf8')

# 爬取线程结束标识
exitCrawlFlag = False
# 创建线程锁
lock = threading.Lock()
# 存放数据
data_queue = Queue()
# 开启日志系统
log = Logger().getlog()


class Item():
    """
    存储数据的结构
    """
    def __init__(self):
        self.title = None
        self.rating = 0.0
        self.people_num = 0
        self.author_info = None
        self.pub_info = None
        self.href_info = None


class threadCrawl(threading.Thread):
    '''
    可以利用多个线程从豆瓣爬取数据
    '''
    def __init__(self, threadId, q):
        threading.Thread.__init__(self)
        self.threadId = threadId
        self.q = q

    def run(self):
        log.info("starting threadID :{Id}".format(Id=self.threadId))
        self.getWebData()
        log.info("ending threadID :{Id}".format(Id=self.threadId))

    def getWebData(self):
        """
        根据url获取网页
        return: 返回网页信息
        """
        while True:
            if self.q.empty():
                break
            else:
                url = q.get()
                fakeHeaders = {'User-Agent': self.getRandomHeaders()}
                request = urllib2.Request(url, headers=fakeHeaders)
                proxy = urllib2.ProxyHandler({'http': 'http://' + self.getRandomProxy()})
                opener = urllib2.build_opener(proxy)
                urllib2.install_opener(opener)

                try:
                    time.sleep(random.choice([1, 2, 3]))
                    response = urllib2.urlopen(request)
                except Exception, e:
                    log.info('URL:%s  请求数据失败' % url)
                    log.Error("Bad Thing:", e)
                else:
                    self.spider_parse(response)

    def spider_parse(self, responseContent):
        """
        分析爬取的内容
        """
        if responseContent is None:
            return
        soup = BeautifulSoup(responseContent, 'lxml')
        tags = soup.find_all('li', {'class': 'subject-item'})
        for tag in tags:
            item = Item()
            item.title = tag.find('div', {'class': 'info'}).a['title'].strip()
            item.href_info = tag.find('div', {'class': 'info'}).a['href']
            try:
                descList = tag.find('div', {'class': "pub"}).get_text().strip().split("/")
                descLen = len(descList)
                if descLen < 3:
                    item.author_info = "/".join(descList)
                    item.pub_info = "出版信息:暂无"
                else:
                    item.author_info = "/".join(descList[0:-3])
                    item.pub_info = "/".join(descList[-3:])
            except:
                item.author_info = "作者/译者:暂无"
                item.pub_info = "出版信息:暂无"
            people = filter(str.isdigit, (tag.find('span', {'class': 'pl'}).get_text().strip()).encode())
            if people is '':
                item.people_num = 0
            else:
                item.people_num = int(people)

            try:
                rating = tag.find('span', {'class': 'rating_nums'}).get_text()
                item.rating = float(rating.encode())
            except:
                item.rating = 0.0

            data_queue.put(item)

    def getRandomProxy(self):
        """
        随机选取proxy代理
        """
        return random.choice(resource.PROXIES)

    def getRandomHeaders(self):
        """
        随机选取文件头
        """
        return random.choice(resource.UserAgents)


class threadSaveData(threading.Thread):
    """
    保存数据线程
    """
    def __init__(self, theadId, keyword='编程'):
        threading.Thread.__init__(self)
        self.threadId = theadId
        self.keyword = keyword

    def run(self):
        log.info("starting {threadid}".format(threadid=self.threadId))
        self.pipelines()
        log.info("endinging {threadid}".format(threadid=self.threadId))

    def readToTxt(self):
        """
        将爬取的数据写入到txt文件，写入时加锁
        """
        fileName = self.keyword + '.txt'
        global exitCrawlFlag
        while (not exitCrawlFlag) or (not data_queue.empty()):
            try:
                item = data_queue.get(False)
                with lock:
                    with open(fileName, "a") as fp:
                        fp.write('%s\t%s\t%s\t%s\t%s\t%s\n' %
                                 (item.title, item.rating, item.people_num, item.author_info,
                                  item.pub_info, item.href_info))
                data_queue.task_done()
            except:
                pass

    def pipelines(self):
        """
        处理获取的数据
        """
        self.readToTxt()


def readToExcel(keyword):
    """
    从txt文件读取爬取的内容到Excel
    """
    fileName = keyword + '.txt'

    wb = Workbook()
    ws = wb.create_sheet(keyword.decode(), 0)
    ws.append(["书名", "评分", "评论人数", "作者/译者", "出版社", "图书详情"])
    with open(fileName, 'r') as fp:
        line = fp.readline()
        while line:
            ws.append(line[:-1].split("\t"))
            line = fp.readline()

    wb.save("douban_search.xlsx")


def readtoMysql(keyword):
    """
    从txt文件读取爬取的内容到Mysql
    """
    fileName = keyword + '.txt'
    DB_NAME = 'Douban'
    TABLE_NAME = 'Douban_book'

    conn = MySQLdb.connect(**resource.MysqlConfig)
    cur = conn.cursor()
    cur.execute('DROP DATABASE IF EXISTS %s' % DB_NAME)
    cur.execute('CREATE DATABASE IF NOT EXISTS %s CHARACTER SET "utf8" ' % DB_NAME)
    conn.select_db(DB_NAME)
    cur.execute('CREATE TABLE %s(   \
                      id int primary key not null auto_increment, \
                      book       varchar(50),  \
                      rating     float(3.1), \
                      people_num int,  \
                      author     varchar(120), \
                      publicInfo     varchar(80), \
                      book_info  varchar(80)  \
                   )' % TABLE_NAME)

    with open(fileName, 'r') as fp:
        line = fp.readline()
        while line:
            value = line[:-1].split("\t")
            value[1] = float(value[1])
            value[2] = int(value[2])
            cur.execute('insert into Douban_book(book,rating, people_num, author, publicInfo, book_info) \
                        values(%s, %s, %s, %s, %s, %s)', value)
            line = fp.readline()
    cur.close()
    conn.commit()
    conn.close()


def queue_urls(keyword, page=1):
    """
    将需要爬取的url放入队列
    """
    q_urls = Queue()
    start_id = page * 20
    urlBbase = 'https://book.douban.com/tag/{KeyWord}?start={StartId}'
    key_word = ("+").join(keyword.split())
    for start_id in range(0, start_id, 20):
        url = urlBbase.format(KeyWord=key_word, StartId=start_id)
        q_urls.put(url)
        print url
    return q_urls


if __name__ == '__main__':
    word = "编程"
    pages = 1
    q = queue_urls(word, page=pages)
    """初始化采集线程"""
    crawThreads = []
    crawList = ['T1-crawl', 'T2-crawl', 'T3-crawl', 'T4-crawl']
    for threadId in crawList:
        thread = threadCrawl(threadId, q)
        thread.start()
        crawThreads.append(thread)

    """初始化保存数据线程"""
    saveDataThreads = []
    savaList = ['T1-sava', 'T2-sava']
    for threadId in savaList:
        thread = threadSaveData(threadId, word)
        thread.start()
        saveDataThreads.append(thread)

    """等待采集线程结束"""
    while not q.empty():
        pass
    for t in crawThreads:
        t.join()
    """采集线程结束，通知保存数据线程"""
    exitCrawlFlag = True

    """等待保存数据线程结束"""
    while not data_queue.empty():
        pass
    for t in saveDataThreads:
        t.join()

    """保存到execl"""
    readToExcel(word)
    """保存到数据库"""
    readtoMysql(word)
