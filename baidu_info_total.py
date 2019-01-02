# coding:utf-8
__author__ = 'xxj'

import requests
import time
import datetime
import os
import re
import multiprocessing
from multiprocessing import Process, Pool, Manager
from Queue import Empty
import bs4
import math
from bs4 import BeautifulSoup
from jparser import PageModel
from requests.exceptions import ReadTimeout, ConnectTimeout, ConnectionError
import sys

reload(sys)
sys.setdefaultencoding('utf-8')


def baidu_info(KEYWORD_QUEUE, lock, tmp_file_name):
    headers = {
        'Cookie': 'BAIDUID=FEE0FAFC316E4B8EF23E777A8D4AB72F:FG=1;',
        'Referer': 'https://www.baidu.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36',
    }
    fileout = open(tmp_file_name, 'a')

    while not KEYWORD_QUEUE.empty():
        try:
            line = KEYWORD_QUEUE.get(False)
            data_ls = line.split('<&>')
            keyword = data_ls[0]  # 源数据
            pn = data_ls[1]    # 页码
            url = 'https://www.baidu.com/s?rtt=1&bsst=1&cl=2&tn=news&word={word}&pn={pn}'.format(word='游戏 ' + keyword,
                                                                                                 pn=pn)
            print time.strftime('[%Y-%m-%d %H:%M:%S]'), '索引页url：', url
            response = requests.get(url=url, headers=headers, timeout=10)
            soup = BeautifulSoup(response.text, 'lxml')
            search_total_ls = soup.select('span.nums')
            if search_total_ls:
                search_total = search_total_ls[0].string
                # print 'search_total：', search_total
                search_obj = re.search(r'\d+,*\d*', search_total.encode('utf-8'), re.S)  # 关键词的资讯搜索结果数量
                if search_obj:
                    search_total = search_obj.group().replace(',', '')
                    print '总的搜索量：', search_total
                else:
                    search_total = '0'
                    print '获取资讯总的搜索量的正则表达式失效'
                if (int(search_total) <= 10) and (int(pn) == 10):  # 去除重复
                    print '该游戏无第二页'
                    continue
            else:
                print time.strftime('[%Y-%m-%d %H:%M:%S]'), '无法获取搜索结果量节点元素', url
            results = soup.select('div.result')
            for result in results:
                result_index = result.attrs['id'].strip()  # 索引值
                # print '索引值：', result_index
                title = result.select('h3.c-title > a')[0].text.replace('\r', '').replace('\n', '').replace('\t', '').strip()  # 新闻资讯标题
                # print '新闻资讯标题：', title
                detail_link = result.select('h3.c-title > a')[0].attrs['href']  # 新闻资讯详情页
                # print '新闻资讯详情页：', detail_link
                source_time = result.select('p.c-author')[0].text.strip()  # 来源与时间字段
                # print '来源与时间字段：', repr(source_time)
                source_time_ls = source_time.split('\n')
                # print source_time_ls
                if len(source_time_ls) == 2:
                    source = source_time_ls[0].replace('\r', '').replace('\n', '').replace('\t', '').strip()  # 资讯来源
                    news_time = source_time_ls[1].replace('\r', '').replace('\n', '').replace('\t',
                                                                                              '').strip()  # 资讯发布时间
                else:
                    source = ''  # 资讯来源
                    news_time = source_time_ls[0].replace('\r', '').replace('\n', '').replace('\t',
                                                                                              '').strip()  # 资讯发布时间
                # print '资讯来源：', source
                # print '资讯发布时间：', news_time

                detail_content = baidu_info_detail(detail_link)  # 详情页接口
                # 来源字段、搜索量、序号、标题、url、来源、时间、正文内容
                content = '\t'.join([keyword, search_total, result_index, title, detail_link, source, news_time, detail_content])
                fileout.write(content)
                fileout.write('\n')
                fileout.flush()

        except Empty as e:
            print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'empty'

        except ConnectTimeout as e:
            with lock:
                print time.strftime('[%Y-%m-%d %H:%M:%S]'), '索引页异常ConnectTimeout', url, line
                KEYWORD_QUEUE.put(line)

        except ConnectionError as e:
            with lock:
                print time.strftime('[%Y-%m-%d %H:%M:%S]'), '索引页异常ConnectionError', url, line
                KEYWORD_QUEUE.put(line)

        except ReadTimeout as e:
            with lock:
                print time.strftime('[%Y-%m-%d %H:%M:%S]'), '索引页异常ReadTimeout', url, line
                KEYWORD_QUEUE.put(line)

        except BaseException as e:
            print time.strftime('[%Y-%m-%d %H:%M:%S]'), '索引页异常BaseException', url, e


def baidu_info_detail(detail_link):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36',
        }
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), '详情页url：', detail_link
        response = requests.get(detail_link, headers=headers, timeout=10)
        if response.status_code == 200:
            # response.encoding = response.apparent_encoding
            search_obj = re.search(r'<meta.*?charset="?(.*?)"', response.text, re.S)
            if search_obj:
                charset = search_obj.group(1)
                response.encoding = charset
            else:
                content = ''
                print '解析新的页面编码meta字符串', detail_link
                return content

            contents = []
            pm = PageModel(response.text)
            result = pm.extract()
            for x in result['content']:
                if x['type'] == 'text':
                    p = x['data'].replace('\r', '').replace('\n', '').replace('\t', '').strip()
                    contents.append(p)
            content = ''.join(contents)

        else:
            print 'response status is not 200', response.status_code
            content = ''

    except ConnectTimeout as e:
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), '详情页异常ConnectTimeout', detail_link
        content = ''

    except ConnectionError as e:
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), '详情页异常ConnectionError', detail_link
        content = ''

    except ReadTimeout as e:
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), '详情页异常ReadTimeout', detail_link
        content = ''

    except BaseException as e:
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), '详情页异常BaseException', detail_link, e
        content = ''
    return content


def main():
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'start'
    lock = Manager().Lock()
    KEYWORD_QUEUE = Manager().Queue()  # 来源队列
    PROXY_IP_Q = Manager().Queue()  # 代理队列
    DATA_QUEUE = Manager().Queue()  # 数据队列
    yesterday = datetime.date.today() + datetime.timedelta(-1)
    date = yesterday.strftime('%Y%m%d')
    file_date = time.strftime('%Y%m%d')
    keyword_file_dir = r'/ftp_samba/112/file_4spider/dmn_fanyule2_game/'  # 游戏的来源目录
    keyword_file_name = r'dmn_fanyule2_game_{date}_1.txt'.format(date=date)  # 游戏的来源文件名
    keyword_file_path = os.path.join(keyword_file_dir, keyword_file_name)
    # keyword_file_path = r'C:\Users\xj.xu\Desktop\dmn_fanyule2_game_20181202_1.txt'
    # keyword_file_path = r'/home/spider/fanyule_two/dmn_fanyule2_game_20181217_1.txt'
    print '获取来源文件：', keyword_file_path
    while True:
        if os.path.exists(keyword_file_path):
            break
        time.sleep(60)
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), '游戏文件路径：', keyword_file_path
    keyword_file = open(keyword_file_path, 'r')
    for line in keyword_file:
        line = line.strip()
        if line:
            for page in [i * 10 for i in xrange(2)]:
                new_line = '<&>'.join([line, str(page)])
                KEYWORD_QUEUE.put(new_line)
    print '数据来源关键词的数量：', KEYWORD_QUEUE.qsize()

    dest_path = '/ftp_samba/112/spider/fanyule_two/baidu/'  # linux上的文件目录
    # dest_path = os.getcwd()    # windows上的文件目录
    if not os.path.exists(dest_path):
        os.makedirs(dest_path)
    dest_file_name = os.path.join(dest_path, 'baidu_infor_total_' + file_date)
    tmp_file_name = os.path.join(dest_path, 'baidu_infor_total_' + file_date + '.tmp')

    pool = Pool(15)
    for i in xrange(15):
        pool.apply_async(baidu_info, args=(KEYWORD_QUEUE, lock, tmp_file_name))

    pool.close()
    pool.join()

    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'end'
    os.rename(tmp_file_name, dest_file_name)


if __name__ == '__main__':
    main()
