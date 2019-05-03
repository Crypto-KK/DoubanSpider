from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
import csv

from lxml import etree
import pymysql
import requests

from models import create_session, Comments


class CommentFetcher:

    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36'}
    cookie = ''
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

            print('正在获取{} 开始的记录'.format(self.start))
        except Exception as e:
            print('访问已达到最大次数，请更换IP地址')

        return res

    def _parse(self):
        res = self._get()
        dom = etree.HTML(res)
        self.id = dom.xpath(self.base_node + '/@data-cid')
        self.username = dom.xpath(self.base_node + '/div[@class="avatar"]/a/@title')
        self.user_center = dom.xpath(self.base_node + '/div[@class="avatar"]/a/@href')

        self.vote = dom.xpath(self.base_node + '//span[@class="votes"]/text()')
        # self.star = dom.xpath(self.base_node + '//span[contains(@class,"rating")]/@title')
        self.time = dom.xpath(self.base_node + '//span[@class="comment-time "]/@title')
        self.content = dom.xpath(self.base_node + '//span[@class="short"]/text()')

    def save_to_database(self):

        self._parse()
        try:
            comments = [Comments(
                id=int(self.id[i]),
                username=self.username[i],
                user_center=self.user_center[i],
                vote=int(self.vote[i]),
                # star=self.star[i],
                time=datetime.strptime(self.time[i], '%Y-%m-%d %H:%M:%S'),
                content=self.content[i]
            ) for i in range(len(self.username))]
            self.session.add_all(comments)
            self.session.commit()
            return 'finish'

        except pymysql.err.IntegrityError as e:
            print('pymysql错误')
            pass

        except Exception as e:
            print(e)
            raise e
            print('rollback')
            self.session.rollback()


        finally:
            self.session.close()

    def save_to_csv(self):
        self._parse()
        f = open('test.csv', 'w', encoding='utf-8')
        csv_in = csv.writer(f, dialect='excel')
        for i in range(len(self.id)):
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

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for i in range(25):
            fetcher = CommentFetcher(movie_id=26266893, start=i * 20, type='l')
            futures.append(executor.submit(fetcher.save_to_csv()))

        for f in as_completed(futures):
            try:
                res = f.done()
                if res:
                    ret_data = f.result()
                    if ret_data == 'finish':
                        print('{} 成功保存到数据库'.format(str(f)))
            except Exception as e:
                f.cancel()




