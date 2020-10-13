# -*- coding:utf-8 -*-
import requests
import re
import json
import queue
import threading
import time
import sqlite3

# GET请求
ROOM_ID = '12735949'
URL_GET = 'http://api.live.bilibili.com/AppRoom/msg?room_id='
HEAD = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36'
}

ROOM_INFO_URL = 'https://api.live.bilibili.com/room/v1/Room/room_init?id='
# 保存地址

DB_BULLET_COMMENTS_SAVE_PATH = './' + ROOM_ID + '.db'

class GetBullets(object):
    def __init__(self):
        self.Queue_data = queue.Queue()  # 弹幕队列
        self.exist_info = list()  # [info_list]

    def get_room_info(self,):
        '''获取房间信息并处理'''
        room_info = {}
        try:
            check_response = requests.get(ROOM_INFO_URL + ROOM_ID)
            check_response.encoding = 'utf-8'
            room_info = json.loads(check_response.text)['data']
        except requests.HTTPError as e:
            if hasattr(e, 'code'):
                print('错误代码为：',e.code)
            if hasattr(e, 'reason'):
                print('错误原因为：',e.reason)
        return room_info

    def get_true_room_id(self):
        '''获取房间真实id'''
        room_info = self.get_room_info()
        room_id = ROOM_ID
        if room_info['short_id'] == 0:
            pass
        else:
            room_id = str(room_info['room_id'])
        return room_id

    def get_room_status(self):
        '''检查主播是否在播'''
        room_info = self.get_room_info()
        live_status = 1
        if room_info['live_status'] == 1:
            pass
        else:
            live_status = 0
        return live_status

    def get_comments(self, url):
        '''加载弹幕'''
        room_id = self.get_true_room_id()
        bullet_comments = ''
        url = url + room_id
        try:
            response = requests.get(url, headers=HEAD)  # GET请求
            response.encoding = 'utf-8'
            bullet_comments = response.text
        except requests.HTTPError as e:
            if hasattr(e, 'code'):
                print('错误代码为：',e.code)
            if hasattr(e, 'reason'):
                print('错误原因为：',e.reason)
        return bullet_comments

    def bullet_set(self):
        '''提取弹幕中的信息'''
        while True:
            live_status = self.get_room_status()
            if not live_status:
                self.Queue_data.put('over')
                break
            bullet_comments = self.get_comments(URL_GET)  # GET请求
            bullet_comments_data = re.findall(r'\"data\":(.+?)\"message\"', bullet_comments)[0]
            bullet_comments_data = json.loads(bullet_comments_data.strip(','))
            # 房管
            data_vip = bullet_comments_data['admin']  # list类型,房管的弹幕
            info_list = []
            if len(data_vip) != 0:
                for info in data_vip:
                    text = info['text']
                    user_id = info['uid']
                    user_name = info['nickname']
                    time_line = info['timeline']
                    is_admin = '1'
                    info_list = [user_id, user_name, text, time_line, is_admin]
                    if info_list[:-1] in self.exist_info:
                        continue
                    else:
                        self.Queue_data.put(info_list)
                        self.exist_info.append(info_list[:-1])

            # 普通
            data_normal = bullet_comments_data['room']  # 非房管的弹幕
            if len(data_normal) != 0:
                for info in data_normal:
                    text = info['text']
                    user_id = info['uid']
                    user_name = info['nickname']
                    time_line = info['timeline']
                    is_admin = '0'
                    info_list = [user_id, user_name, text, time_line, is_admin]
                    if info_list[:-1] in self.exist_info:
                        continue
                    else:
                        self.Queue_data.put(info_list)
                        self.exist_info.append(info_list[:-1])
                    if len(self.exist_info) > 30:
                        del self.exist_info[:15]
            time.sleep(2)

    def save(self):
        '''保存弹幕信息'''
        counts = 0
        # 数据库建表
        sql = '''
            create table bulletcomments
            (user_id text,
            user_name text,
            content text,
            time_line text,
            is_admin text
            )
        '''  # 创建数据表
        conn = sqlite3.connect(DB_BULLET_COMMENTS_SAVE_PATH)  # 链接数据库
        cursor = conn.cursor()  # 游标
        try:
            cursor.execute(sql)  # 执行操作
        except sqlite3.OperationalError:
            pass

        # 开始保存弹幕信息
        while True:
            if not self.Queue_data.empty():
                danmu = self.Queue_data.get()
                if danmu == 'over':
                    print('主播下播，爬取结束。')
                    break
                counts += 1
                # 保存至数据库
                sql_insert = '''
                insert into bulletcomments (user_id,user_name,content,time_line,is_admin) values ("%s","%s","%s","%s","%s")
                '''%(danmu[0],danmu[1],danmu[2],danmu[3],danmu[4])
                cursor.execute(sql_insert)
                conn.commit()  # 提交事务
                print('已抓取%d条弹幕' % counts)
        conn.close()  # 关闭数据库

    def run(self):
        download = threading.Thread(target=self.bullet_set)  # 弹幕爬取
        save = threading.Thread(target=self.save)  # 弹幕保存

        download.start()  # 开启爬取
        save.start()  # 保存弹幕

if __name__ == '__main__':
    GetBullets().run()