# -*- coding : utf-8 -*- #
import datetime
import re
import requests
from dateutil.relativedelta import relativedelta

import Contants
from StockList import StockList


class DownloadYearReport:
    def __init__(self, code, org_id):
        self.saving_path = Contants.REPORT_DIR  # 设置存储年报的文件夹
        self.code = code
        self.orgId = org_id

    def get_and_download_pdf_file(self, pageNum):
        url = 'http://www.cninfo.com.cn/new/hisAnnouncement/query'
        page_num = int(pageNum)
        now = datetime.date.today()
        stock = self.code + ',' + self.orgId
        seDate = (now - relativedelta(years=3)).strftime('%Y-%m-%d') + "~" + now.strftime('%Y-%m-%d')
        print(seDate)
        data = {'pageNum': page_num,
                'pageSize': 30,
                'column': 'szse',
                'tabName': 'fulltext',
                'plate': '',
                'stock': stock,
                'searchkey': '',
                'secid': '',
                'category': 'category_ndbg_szsh',
                'trade': '',
                'seDate': seDate,
                'sortName': '',
                'sortType': '',
                'isHLtitle': 'true'}
        headers = {'Accept': '*/*',
                   'Accept-Encoding': 'gzip, deflate',
                   'Accept-Language': 'zh-CN,zh;q=0.9',
                   'Connection': 'keep-alive',
                   'Content-Length': '181',
                   'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                   'Host': 'www.cninfo.com.cn',
                   'Origin': 'http://www.cninfo.com.cn',
                   'Referer': 'http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search',
                   'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
                   'X-Requested-With': 'XMLHttpRequest'}
        r = requests.post(url, data=data, headers=headers)
        result = r.json()['announcements']  # 获取单页年报的数据，数据格式为json。获取json中的年报信息。
        if result is None:
            print("result is none exit!!!!! pageNum = " + str(pageNum))
            exit(0)
        # 2.对数据信息进行提取
        for i in result:
            if re.search('摘要', i['announcementTitle']) or re.search('英文版',
                                                                      i['announcementTitle']):  # 避免下载一些年报摘要等不需要的文件
                pass
            else:
                title = i['announcementTitle']
                secName = i['secName']
                secName = secName.replace('*', '')  # 下载前要将文件名中带*号的去掉，因为文件命名规则不能带*号，否则程序会中断
                secCode = i['secCode']
                adjunctUrl = i['adjunctUrl']
                down_url = 'http://static.cninfo.com.cn/' + adjunctUrl
                filename = f'{secCode}{secName}{title}.pdf'
                filepath = self.saving_path + '/' + filename
                r = requests.get(down_url)
                with open(filepath, 'wb') as f:
                    f.write(r.content)
                print(f'{secCode}{secName}{title}下载完毕')  # 设置进度条


if __name__ == '__main__':
    name = "洛阳钼业"
    stockList = StockList()
    stock = stockList.get_stock_by_name(name)
    if stock is None:
        print("not found ! name = " + name)
        exit(1)
    print(stock.code, stock.orgId)
    download = DownloadYearReport(stock.code, stock.orgId)
    # 3.设置循环，下载多页的年报
    for pageNum in range(1, 3):
        download.get_and_download_pdf_file(pageNum)
