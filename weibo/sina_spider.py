"""
用于爬取用户信息
@author:ACool(www.github.com/starFalll)
爬取微博用户资料和动态并保存在数据库中
"""
import csv

import requests
import random
import re
import pymysql
import time
from weibo.Connect_mysql import Connect

# 处理cookies
def getcookies(cookie):
    cookies = {}
    for line in cookie.split(';'):
        name, value = line.strip().split('=', 1)
        cookies[name] = value
    return cookies

# 抓取html
def gethtml(url, headers, cookie, conf, use_proxies=False):
    print(url)
    try:
        if (use_proxies == True):  # 使用代理
            proxy = conf.get('IP')
            proxy = list(proxy.values())
            ip = random.choice(proxy)
            proxies = {"http": "http://" + str(ip), }
            r = requests.get(url, headers=headers, cookies=cookie, timeout=10, proxies=proxies)
            time.sleep(random.random() + 0.5)  # 随机延迟0.5~1.5秒
            if r.status_code != 200:
                raise Exception('界面获取失败:return ' + str(r.status_code))
            else:
                return r
        else:
            r = requests.get(url, headers=headers, cookies=cookie, timeout=10)
            time.sleep(random.random() + 0.5)  # 随机延迟0.5~1.5秒
            if r.status_code != 200:
                raise Exception('界面获取失败')
            else:
                return r
    except Exception as e:
        print(e)
    # 如果抓取超时则抓取baidu404界面，r会是空的
    r = requests.get('https://www.baidu.com/search/error.html', headers=headers)
    return r


# 获取个人资料并导入mysql
def getinfo(r, uid):
    tip = re.compile(r'class="tip">(.*?)></div>', re.S)
    title = re.compile(r'(.*?)</div><div', re.S)  # 基本信息/学历信息
    node = re.compile(r'.*?class="c"(.*?)$', re.S)
    info = re.compile(r'>(.*?)<br/', re.S)
    tips = re.findall(tip, r.text)
    Uname = ''
    Certified = ''
    Sex = ''
    Relationship = ''
    Area = ''
    Birthday = ''
    Education_info = ''
    Work_info = ''
    Description = ''
    for one in tips:
        try:
            titleone = re.findall(title, one)  # 信息标题

            node_tmp = re.findall(node, one)
            infos = re.findall(info, node_tmp[0])  # 信息
            if (titleone[0] == '基本信息'):
                for inf in infos:
                    if (inf.startswith('昵称')):
                        _, Uname = inf.split(':', 1)
                    elif (inf.startswith('认证信息')):
                        print(inf)
                        _, Certified = inf.split('：', 1)
                    elif (inf.startswith('性别')):
                        _, Sex = inf.split(':', 1)
                    elif (inf.startswith('感情状况')):
                        _, Relationship = inf.split(':', 1)
                    elif (inf.startswith('地区')):
                        _, Area = inf.split(':', 1)
                    elif (inf.startswith('生日')):
                        _, Birthday = inf.split(':', 1)
                    elif (inf.startswith('简介')):
                        print(inf.split(':'))
                        _, Description = inf.split(':', 1)
                    else:
                        pass
            elif (titleone[0] == '学习经历'):
                for inf in infos:
                    Education_info += inf.strip('·').replace("&nbsp", '') + " "
            elif (titleone[0] == '工作经历'):
                for inf in infos:
                    Work_info += inf.strip('·').replace("&nbsp", '') + " "
            else:
                pass
        except Exception as e:
            pass
        continue

    user_info = [uid, Uname, Certified, Sex, Relationship, Area, Birthday, Education_info, Work_info, Description]
    print(user_info)
    return user_info

# 获取个人动态信息并导入mysql
def getmain(res, uid, url, user_agents, cookies, conf, use_proxies=False):
    dynamic = re.compile(r'.*?><span class="ctt">(.*?)<a href', re.S)  # 匹配动态
    times = re.compile(r'.*?<span class="ct">(.*?)&nbsp', re.S)  # 匹配动态发布时间
    page_number = re.compile(r'.*/(\d*?)页</div>', re.S)  # 匹配动态页数
    re_nbsp = re.compile(r'&nbsp', re.S)  # 去除$nbsp
    re_html = re.compile(r'</?\w+[^>]*>', re.S)  # 去除html标签
    re_200b = re.compile(r'\u200b', re.S)  # 去除分隔符
    re_quot = re.compile(r'&quot', re.S)
    dys = re.findall(dynamic, res.text)
    ts = re.findall(times, res.text)
    pages = re.findall(page_number, res.text)
    if (len(pages) <= 0):
        print('\033[1;31mERROR!!! uid:' + str(uid) + ' does not have page_number tags. Skip this uid...\033[0m')
        return
    pagenums = pages[0]
    print(pagenums)

    print("dys:")
    for d in dys: print(pymysql.converters.escape_string(d))

    print("ts:")
    for t in ts: print(t)

    mainurl = url
    label = 0  # 标签用于计数，每十次延时10S
    for pagenum in range(int(pagenums))[1:20]:
        if (label == 10):
            time.sleep(10)
            label = 0
        # 随机选择，防止被ban
        cookie = random.choice(cookies)
        cookie = getcookies(cookie)
        headers = {
            'User_Agent': random.choice(user_agents)
        }
        pagenum += 1
        label += 1
        url = mainurl + '?page=' + str(pagenum)
        page = gethtml(url, headers, cookie, conf, use_proxies)
        dys += re.findall(dynamic, page.text)
        ts += re.findall(times, page.text)
    dys = dys[1:]
    print(len(dys))
    print(len(ts))
    for i in range(len(ts)):
        dys[i] = re_nbsp.sub('', dys[i])
        dys[i] = re_html.sub('', dys[i])
        dys[i] = re_200b.sub('', dys[i])
        dys[i] = re_quot.sub('', dys[i])
    return dys, ts


# 默认不使用代理ip
def main(use_proxies=False):
    conf, engine = Connect('conf.yaml')  # 获取配置文件的内容
    # uids = conf.get('uids')
    cookies = conf.get('cookies')
    user_agents = conf.get('user_agents')
    # uids = list(uids.values())
    cookies = list(cookies.values())
    user_agents = list(user_agents.values())

    # uids = ['3774950943','1868936137']

    uids = []
    uidFilePath = "user_id_list.txt"
    uidFile = open(uidFilePath,'r')
    for line in uidFile:
        uids.append(line.strip())

    # userInfoPath = "collected_data/user_info.csv"
    userInfoPath = "collected_data/user_info_append.csv"
    userInfoFile = open(userInfoPath, 'w', encoding='utf-8', newline='')
    user_writer = csv.writer(userInfoFile)
    user_writer.writerow(
        ["uid", "Uname", "Certified", "Sex", "Relationship", "Area", "Birthday", "Education_info", "Work_info",
         "Description"])

    for uid in uids:
        # 随机选择，防止被ban
        cookie = random.choice(cookies)
        cookie = getcookies(cookie)
        headers = {
            'User_Agent': random.choice(user_agents)
        }
        infourl = 'https://weibo.cn/' + str(uid) + '/info'  # 资料页面
        # mainurl = 'https://weibo.cn/' + str(uid)  # 动态页面
        resinfo = gethtml(infourl, headers, cookie, conf, use_proxies)  # 抓取资料页的信息
        # resmain = gethtml(mainurl, headers, cookie, conf, use_proxies)  # 抓取用户主页信息
        info = getinfo(resinfo, uid)
        user_writer.writerow(info)

        # dataInfoPath = "collected_data/text_data/"+str(uid)+ ".csv"
        # dataInfoFile = open(dataInfoPath, 'w', newline='',encoding='utf-8')
        # data_writer = csv.writer(dataInfoFile)
        # data_writer.writerow(["weibo_cont", "create_time"])
        # dys,ts = getmain(resmain, uid,mainurl, user_agents, cookies, conf, use_proxies)
        # for i in range(len(ts)):
        #     dys[i] = pymysql.converters.escape_string(dys[i])
        #     if len(dys[i])!=0 and '原文转发' not in dys[i]:
        #         data_writer.writerow([dys[i],ts[i]])
        # dataInfoFile.close()

    userInfoFile.close()

if __name__ == '__main__':
    main(use_proxies=False)  # 默认不使用代理ip
