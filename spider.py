from datetime import datetime

from lxml import etree
import pymysql
import requests

from models import create_session, Comments


class CommentFetcher:

    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36'}
    cookie = 'll="118281"; bid=JW9ceOtkzVQ; __utmc=30149280; __utmz=30149280.1556887217.1.1.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; ap_v=0,6.0; __yadk_uid=4Qc0m9Yphgw7SrHQHicZBbM269AUCowI; push_noty_num=0; push_doumail_num=0; __utmv=30149280.19592; _pk_ref.100001.8cb4=%5B%22%22%2C%22%22%2C1556890576%2C%22https%3A%2F%2Fwww.baidu.com%2Flink%3Furl%3DfBPHI-eF--4C1vgnaxPkJOAHIFxxo7osPOKBVz_FwSO%26wd%3D%26eqid%3Dc27dab0d00213fbd000000065ccc36a5%22%5D; _pk_ses.100001.8cb4=*; __utma=30149280.1933236491.1556887217.1556887217.1556890577.2; _vwo_uuid_v2=D6231F23D873FE21CE33FE82E12899FB3|0b57003a81123ce8df6e42ca951de47f; __utmt=1; dbcl2="195925767:zfIHw/L/COw"; ck=qHHm; _pk_id.100001.8cb4=cd248ea63d8d938d.1556887213.2.1556891683.1556887266.; __gads=ID=91c2f88c9eec95d1:T=1556891681:S=ALNI_MZDvckmgPiDRE9BrmUOq-pxtctaIw; __utmb=30149280.8.10.1556890577'
    cookies = {'cookie': cookie}
    base_node = '//div[@class="comment-item"]'
    #cookie为登录后的cookie，需要自行复制

    def __init__(self, movie_id, start, type=''):
        '''
        :type: 全部评论：空， 好评：h 中评：m 差评：l
        :movie_id: 影片的ID号
        :start: 开始的记录数
        '''
        self.movie_id = movie_id
        self.start = start
        self.type = type
        self.url = 'https://movie.douban.com/subject/{id}/comments?start={start}&limit=20&sort=new_score\&status=P&percent_type={type}&comments_only=1'.format(
            id=str(self.movie_id),
            start=str(self.start),
            type=self.type
        )
        self.session = create_session()

    def _get(self):
        res = ''
        try:
            res = requests.get(self.url, cookies=self.cookies, headers=self.headers)
            res = res.json()['html']
        except Exception as e:
            print('IP被封，请使用代理IP')
        print('正在获取{} 开始的记录'.format(self.start))
        return res

    def _parse(self):
        res = self._get()
        dom = etree.HTML(res)

        self.id = dom.xpath(self.base_node + '/@data-cid')
        #用户名
        self.username = dom.xpath(self.base_node + '/div[@class="avatar"]/a/@title')
        #用户连接
        self.user_center = dom.xpath(self.base_node + '/div[@class="avatar"]/a/@href')
        #点赞数
        self.vote = dom.xpath(self.base_node + '//span[@class="votes"]/text()')
        #星级
        self.star = dom.xpath(self.base_node + '//span[contains(@class,"rating")]/@title')
        #发表时间
        self.time = dom.xpath(self.base_node + '//span[@class="comment-time "]/@title')
        #评论内容
        self.content = dom.xpath(self.base_node + '//span[@class="short"]/text()')


    def save(self):
        self._parse()
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
                    content=self.content[i]
                )

                self.session.add(comment)
                self.session.commit()

            except pymysql.err.IntegrityError as e:
                print('数据重复，不做任何处理')

            except Exception as e:
                print('包含特殊字符，数据库回滚')
                self.session.rollback()

            finally:
                #关闭数据库连接
                self.session.close()

if __name__ == '__main__':
    #爬取综合评论、好评、中评、差评
    for i in ['', 'h', 'm', 'l']:
        #最多爬取24页
        for j in range(25):
            fetcher = CommentFetcher(movie_id=26266893, start=j * 20, type=i)
            fetcher.save()
