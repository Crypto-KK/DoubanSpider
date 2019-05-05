from datetime import datetime
import random
import csv

from lxml import etree
import pymysql
import requests

from models import create_session, Comments

#随机UA
USERAGENT = [
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.835.163 Safari/535.1',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:6.0) Gecko/20100101 Firefox/6.0',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50',
    'Opera/9.80 (Windows NT 6.1; U; zh-cn) Presto/2.9.168 Version/11.50',
    'Mozilla/5.0 (Windows; U; Windows NT 6.1; ) AppleWebKit/534.12 (KHTML, like Gecko) Maxthon/3.0 Safari/534.12'
]

class CommentFetcher:
    #以下为类变量
    headers = {'User-Agent': ''}
    #headers为请求头
    cookie = 'll="118281"; bid=JW9ceOtkzVQ; __utmc=30149280; __utmz=30149280.1556887217.1.1.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; ap_v=0,6.0; __yadk_uid=4Qc0m9Yphgw7SrHQHicZBbM269AUCowI; push_noty_num=0; push_doumail_num=0; __utmv=30149280.19592; _pk_ref.100001.8cb4=%5B%22%22%2C%22%22%2C1556890576%2C%22https%3A%2F%2Fwww.baidu.com%2Flink%3Furl%3DfBPHI-eF--4C1vgnaxPkJOAHIFxxo7osPOKBVz_FwSO%26wd%3D%26eqid%3Dc27dab0d00213fbd000000065ccc36a5%22%5D; _pk_ses.100001.8cb4=*; __utma=30149280.1933236491.1556887217.1556887217.1556890577.2; _vwo_uuid_v2=D6231F23D873FE21CE33FE82E12899FB3|0b57003a81123ce8df6e42ca951de47f; __utmt=1; dbcl2="195925767:zfIHw/L/COw"; ck=qHHm; _pk_id.100001.8cb4=cd248ea63d8d938d.1556887213.2.1556891683.1556887266.; __gads=ID=91c2f88c9eec95d1:T=1556891681:S=ALNI_MZDvckmgPiDRE9BrmUOq-pxtctaIw; __utmb=30149280.8.10.1556890577'
    cookies = {'cookie': cookie}
    # cookie为登录后的cookie，需要自行复制
    base_node = '//div[@class="comment-item"]' #所有div下面class属性为comment-item的节点


    #构造函数
    def __init__(self, movie_id, start, type=''):
        '''
        :type: 全部评论：''， 好评：h 中评：m 差评：l
        :movie_id: 影片的ID号
        :start: 开始的记录数，0-480
        '''
        self.movie_id = movie_id
        self.start = start
        self.type = type
        self.url = 'https://movie.douban.com/subject/{id}/comments?start={start}&limit=20&sort=new_score\&status=P&percent_type={type}&comments_only=1'.format(
            id=str(self.movie_id),
            start=str(self.start),
            type=self.type
        )
        #创建数据库连接
        self.session = create_session()

    #随机useragent
    def _random_UA(self):
        self.headers['User-Agent'] = random.choice(USERAGENT)

    #获取api接口，使用get方法，返回的数据为json数据，需要提取里面的HTML
    def _get(self):
        self._random_UA()
        res = ''
        try:
            res = requests.get(self.url, cookies=self.cookies, headers=self.headers)
            res = res.json()['html']
            #返回的数据为json数据，需要提取里面的HTML
        except Exception as e:
            print('IP被封，请使用代理IP')
        print('正在获取{} 开始的记录'.format(self.start))
        return res

    def _parse(self):

        res = self._get()
        dom = etree.HTML(res)

        #id号
        self.id = dom.xpath(self.base_node + '/@data-cid')
        #用户名 所有div节点下class名叫avatar的节点下面的a标签
        self.username = dom.xpath(self.base_node + '/div[@class="avatar"]/a/@title')
        #用户连接
        self.user_center = dom.xpath(self.base_node + '/div[@class="avatar"]/a/@href')
        #点赞数
        self.vote = dom.xpath(self.base_node + '//span[@class="votes"]/text()')
        #星级
        self.star = dom.xpath(self.base_node + '//span[contains(@class,"rating")]/@title')
        #发表时间
        self.time = dom.xpath(self.base_node + '//span[@class="comment-time "]/@title')
        #评论内容 所有span标签class名为short的节点文本
        self.content = dom.xpath(self.base_node + '//span[@class="short"]/text()')
        print(self.content)
    def _get_user_info(self):
        self.address_list = []
        for link in self.user_center:
            try:
                print('正在获取地理位置信息')
                html = requests.get(link, headers=self.headers, cookies=self.cookies)
                dom = etree.HTML(html.text)

                address = dom.xpath('//div[@class="user-info"]/a/text()')[0]
                self.address_list.append(address)
            except Exception as e:
                self.address_list.append('未知')

    #保存到数据库
    def save_to_database(self):
        self._parse()
        #self._get_user_info()
        #数据一条一条插入
        for i in range(len(self.id)):
            try:
                comment = Comments(
                    id=int(self.id[i]),
                    username=self.username[i],
                    user_center=self.user_center[i],
                    vote=int(self.vote[i]),
                    star=self.star[i],
                    time=datetime.strptime(self.time[i], '%Y-%m-%d %H:%M:%S'),
                    content=self.content[i],
                    #address=self.address_list[i],
                )

                self.session.add(comment)
                self.session.commit()
                #提交到数据库

            except pymysql.err.IntegrityError as e:
                print('数据重复，不做任何处理')

            except Exception as e:
                #数据添加错误，回滚
                self.session.rollback()

            finally:
                #关闭数据库连接
                self.session.close()

    #保存到csv
    def save_to_csv(self):
        self._parse()
        #保存在test.csv里面
        f = open('test.csv', 'w', encoding='utf-8')
        csv_in = csv.writer(f, dialect='excel')
        for i in range(len(self.id)):
            #添加一行
            csv_in.writerow([
                int(self.id[i]),
                self.username[i],
                self.user_center[i],
                int(self.vote[i]),
                self.time[i],
                self.content[i]
            ])
        f.close()

if __name__ == '__main__':
    #[综合评论、好评、中评、差评]
    for i in ['', 'h', 'm', 'l']:
        #最多爬取24页
        for j in range(25):
            fetcher = CommentFetcher(movie_id=26363254, start=j * 20, type=i)
            #保存到数据库
            fetcher.save_to_database()
            #保存到csv
            #fetcher.save_to_csv()



