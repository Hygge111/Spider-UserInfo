"""
@author:ACool(www.github.com/starFalll)
连接数据库
"""
from sqlalchemy import create_engine
from yaml import load,Loader

#加载配置
def loadconf_db(file_path):
    with open(file_path,'r',encoding='utf-8') as f:
            cont=f.read()
            cf=load(cont,Loader)
            return cf

def Connect(file):
    conf = loadconf_db(file)
    db = conf.get('db')
    connect_str = str(db['db_type'])+'+pymysql://' + str(db['user']) + ':' + str(db['password']) + \
                  '@'+str(db['host'])+':'+str(db['port'])+'/'+str(db['db_name'])+'?charset=utf8mb4'
    engine = create_engine(connect_str, echo=True)
    # engine = create_engine(connect_str)
    return conf,engine