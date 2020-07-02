"""
@author '彼时思默'
@time 2020/6/29 下午2:50
@describe: 
"""
import os
import arrow
import yaml
from apscheduler.schedulers.blocking import BlockingScheduler
from loguru import logger
import cx_Oracle
import pandas
# logger.remove()
logger.add("./logger/error/error.log",rotation="1 year", enqueue=True, level='ERROR', delay=True)
logger.add("./logger/info/info.log", rotation="1 month", enqueue=True, level='INFO', delay=True)


class OracleJob:
    def __init__(self):
        self.config={}
        self.db = None  # 数据库
        self.cursor = None  # 数据库游标
        self.deadline_time = ''  # 截止时间
        self.default_record={
            'last_execute_time':'1970-01-01 00:00:00',
            'deadline_time':arrow.now().format("YYYY-MM-DD HH:mm:ss")
        }
        self.record={} #控制读取范围的记录

    def prepare(self):
        """
        迁移数据的准备工作,更新连接参数后进行数据库连接;读取进度记录
        :return:
        """
        with open('config.yaml')as file:
            self.config = yaml.safe_load(file)
        self.db_url=f'{self.config["user"]}/{self.config["psw"]}@{self.config["host"]}:{self.config["port"]}/{self.config["db"]}'
        self.db = cx_Oracle.connect(self.db_url)
        self.cursor = self.db.cursor()
        now = arrow.now()
        self.default_record['deadline_time'] = now.format("YYYY-MM-DD HH:mm:ss")

        # 读取记录
        if os.path.exists('./record.yaml'):
            with open('./record.yaml') as file:
                record = yaml.safe_load(file)
                if record is not None:
                    self.record=record


    def export(self):
        """
        导出数据的执行部分
        :return:
        """
        cur_table_name='' #当前表的名字
        try:
            self.prepare()
            for table_name in self.config['tables']:
                cur_table_name=table_name
                table_dict=self.config['tables'][table_name]
                try:
                    if table_name in self.record:
                        record=self.record[table_name]
                        if 'last_execute_time' not in record or record['last_execute_time']=='':
                            record['last_execute_time']=self.default_record['last_execute_time']
                        if 'deadline_time' not in record or record['deadline_time']=='':
                            record['deadline_time']=self.default_record['deadline_time']
                    else:
                        record=self.default_record
                    sql_context = f"select * from {table_name} where {table_dict['field']}>=TO_DATE('{record['last_execute_time']}','YYYY-MM-DD HH24:mi:ss') and {table_dict['field']}<TO_DATE('{record['deadline_time']}','YYYY-MM-DD HH24:mi:ss')"
                    if 'where' in table_dict and table_dict['where'] !='':
                        sql_context+=f'and {table_dict["where"]}'
                    self.cursor.execute(sql_context)
                    rows = self.cursor.fetchall()
                    df = pandas.Dateframe(rows)
                    data_dir = f'/data/export/oracle/{table_name}/'
                    if not os.path.exists(data_dir):
                        os.makedirs(data_dir)
                    df.to_parquet(f"{data_dir}{self.deadline_time}.snappy.parquet", index=False)
                    logger.info(
                        f'table={table_name},last_execute_time={record["last_execute_time"]},deadline_time={self.deadline_time}')
                    self.record[table_name]['last_execute_time']=self.deadline_time
                except Exception as e:
                    logger.error(
                        f'table={table_name},deadline_time={self.deadline_time},e={e}')
            with open('./record.yaml', 'w') as f:
                yaml.safe_dump(self.record, f)
        except Exception as e:
            logger.error(
                f'table={cur_table_name},deadline_time={self.deadline_time},e={e}')

    def start_timer(self):
        """
        定时控制执行
        :return:
        """
        scheduler = BlockingScheduler()
        now = arrow.now()
        next_run_time = now.floor('day').shift(days=1)
        scheduler.add_job(self.export, 'interval', days=1, next_run_time=next_run_time.naive)
        scheduler.start()

if __name__ == '__main__':
    o=OracleJob()
    o.start_timer()