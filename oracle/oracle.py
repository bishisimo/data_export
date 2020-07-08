"""
@author '彼时思默'
@time 2020/7/1 上午10:38
@describe: 
"""
import datetime
import os
import arrow
import yaml
from apscheduler.schedulers.blocking import BlockingScheduler
from loguru import logger
import cx_Oracle
import pandas

# logger.remove()
logger.add("./logger/error/error.log", rotation="1 year", enqueue=True, level='ERROR', delay=True)
logger.add("./logger/info/info.log", rotation="1 month", enqueue=True, level='INFO', delay=True)


class OracleJob:
    def __init__(self):
        self.config = {}
        # self.db = None  # 数据库
        # self.cursor = None  # 数据库游标
        self.default_record = {
            'last_execute_time': '1970-01-01 00:00:00',
            'deadline_time': arrow.now().format("YYYY-MM-DD HH:mm:ss")
        }
        self.record = {}  # 控制读取范围的记录
        self.ancient = {}  # 保存最老的日期
        self.session_pool = None
        self.prepare()

    def prepare(self):
        """
        迁移数据的准备工作,更新连接参数后进行数据库连接;读取进度记录
        :return:
        """
        # 加载配置数据,并创建连接
        with open('config.yaml')as file:
            self.config = yaml.safe_load(file)
        # db_url=f'{self.config["user"]}/{self.config["psw"]}@{self.config["host"]}:{self.config["port"]}/{self.config["db"]}'
        self.session_pool = cx_Oracle.SessionPool(user=self.config["user"], password=self.config["psw"],
                                                  dsn=f'{self.config["host"]}/{self.config["db"]}', min=1, max=10, increment=1)
        # self.db = cx_Oracle.connect(db_url)
        # self.cursor = self.db.cursor()

        # 读取记录
        if os.path.exists('./record.yaml'):
            with open('./record.yaml') as file:
                record = yaml.safe_load(file)
                if record is not None:
                    self.record = record
                else:
                    self.record = {}
        # 读取最早时间记录
        if os.path.exists('ancient.yaml'):
            with open('ancient.yaml')as f:
                self.ancient = yaml.safe_load(f)

    def get_cursor(self):
        db = self.session_pool.acquire()
        return db.cursor()

    def _get_ancient_time(self, table, field):
        """
        获取表中最早数据的时间
        :param table:
        :param field:
        :return:
        """
        sql_content = f'select min({field}) from {table}'
        cursor = self.get_cursor()
        cursor.execute(sql_content)
        row = cursor.fetchone()
        self.ancient[table] = row[0].strftime('%Y-%m-%d %H:%M:%S')
        with open('ancient.yaml', 'w')as f:
            yaml.safe_dump(self.ancient, f)

    def _date2datetime(self, df:pandas.DataFrame, field:str)->pandas.DataFrame:
        def f(x):
            if x is None:
                return x
            try:
                t=arrow.get(x,tzinfo='local')
                return t.format('YYYY-MM-DD HH:mm:ss')
            except:
                return x
        df[field]=df[field].map(f)
        return df

    def export(self,cursor,table_name,table_dict,target_time,deadline_time,mode='daily'):
        """
        导出数据
        :param cursor: 数据库游标
        :param table_name: 表名字
        :param table_dict: 表配置信息
        :param target_time: 导入的目标时间
        :param deadline_time: 导入的截止时间
        :param mode: 导入模式
        :return:
        """
        if 'format' in table_dict:
            oracle_format = self.alter_format(table_dict['format'], 'oracle')
            target_str = target_time.format(self.alter_format(table_dict['format'], 'arrow'))
            deadline_str = deadline_time.format(self.alter_format(table_dict['format'], 'arrow'))
            if 'type' in table_dict and table_dict['type']=='str':
                oracle_date_str=f"{table_dict['field']}>='{target_str}' and {table_dict['field']}<'{deadline_str}'"
            else:
                oracle_date_str = f"{table_dict['field']}>=TO_DATE('{target_str}','{oracle_format}') and {table_dict['field']}<TO_DATE('{deadline_str}','{oracle_format}')"
        else:
            oracle_format = 'YYYY-MM-DD HH24:mi:ss'
            target_str = target_time.format('YYYY-MM-DD HH:mm:ss')
            deadline_str = deadline_time.format('YYYY-MM-DD HH:mm:ss')
            oracle_date_str=f"{table_dict['field']}>=TO_DATE('{target_str}','{oracle_format}') and {table_dict['field']}<TO_DATE('{deadline_str}','{oracle_format}')"
        try:
            sql_context = f"select * from {table_name} where {oracle_date_str}"
            if 'where' in table_dict and table_dict['where'] != '':
                sql_context += f'and {table_dict["where"]}'
            logger.info(sql_context)
            cursor.execute(sql_context)
            rows = cursor.fetchall()
            df = pandas.DataFrame(rows)
            df.columns = [r[0].lower() for r in cursor.description]
            if 'format' in table_dict and table_dict['format']!='second':
                df=self._date2datetime(df,table_dict['field'])
            data_dir = f'/data/export/oracle/{table_name}/'
            if not os.path.exists(data_dir):
                os.makedirs(data_dir)
            if (deadline_time-target_time).days>10:
                file_name=target_time.format('YYYY-MM')
            else:
                file_name=target_time.format('YYYY-MM-DD')
            df.to_parquet(f"{data_dir.lower()}{file_name}.snappy.parquet", index=False)
            if table_name not in self.record:
                self.record[table_name] = {}
            self.record[table_name][f'{mode}_time'] = target_str
            with open('./record.yaml', 'w') as f:
                yaml.safe_dump(self.record, f)
            logger.info(
                f'table={table_name},len={len(df)},target_datetime={target_str}')
        except Exception as e:
            logger.error(
                f'table={table_name},target_datetime={target_str},e={e}')

    def export_daily(self):
        """
        每日导出使用,导出昨天数据
        :return:
        """
        cursor = self.get_cursor()
        now = arrow.now()
        deadline_time = now.floor('day')
        target_time = deadline_time.shift(days=-1)
        for table_name in self.config['tables']:
            table_dict = self.config['tables'][table_name]
            self.export(cursor,table_name,table_dict,target_time,deadline_time)

    def history_export(self):
        """
        历史导出使用,从今天往最远时间进行查询导出
        :return:
        """
        cursor = self.get_cursor()
        for table_name in self.config['tables']:
            target_time = arrow.now().floor('month')
            try:
                table_dict = self.config['tables'][table_name]
                if table_name not in self.ancient:
                    self.ancient[table_name] = self._get_ancient_time(table_name, table_dict['field'])
                while True:
                    deadline_time = target_time
                    target_time = target_time.shift(months=-1)
                    if self.record[table_name]['history_time'] == '':
                        self.record[table_name]['history_time'] = arrow.now().floor('month')
                    history_time = arrow.get(self.record[table_name]['history_time'],tzinfo='local')
                    if history_time < arrow.get(self.ancient[table_name],tzinfo='local') or history_time <= arrow.get('2000-01-01 00:00:00',tzinfo='local'):
                        break
                    self.export(cursor, table_name, table_dict, target_time, deadline_time,'history')
            except Exception as e:
                logger.error(
                    f'table={table_name},target_datetime={target_time},e={e}')

    def latest_export_daily(self):
        """
        最近的数据导出,如没有配置信息从月初开始
        :return:
        """
        cursor = self.get_cursor()
        now=arrow.now()
        for table_name in self.config['tables']:
            target_time = now.floor('month')
            if 'hd_time'  in self.record[table_name]:
                target_time=arrow.get(self.record[table_name]['hd_time'],tzinfo='local')
            try:
                table_dict = self.config['tables'][table_name]
                while True:
                    target_time = target_time.shift(days=1)
                    deadline_time = target_time.shift(days=1)
                    if target_time>=now.floor('day'):
                        break
                    self.export(cursor, table_name, table_dict, target_time, deadline_time,'hd')
            except Exception as e:
                logger.error(
                    f'table={table_name},target_datetime={target_time},e={e}')

    def alter_format(self,format_block,target):
        """
        进行日期格式的转换
        :param format_block: 最低时间格式需求
        :param target: 目标类型
        :return:
        """
        if target=='oracle':
            h='HH24'
            m='mi'
        else:
            h='HH'
            m='mm'
        if format_block=='year':
            return 'YYYY'
        elif format_block=='month':
            return 'YYYY-MM'
        elif format_block=='day':
            return 'YYYY-MM-DD'
        elif format_block=='hour':
            return f'YYYY-MM-DD {h}'
        elif format_block=='month':
            return f'YYYY-MM-DD {h}:{m}'
        elif format_block=='month':
            return f'YYYY-MM-DD {h}:{m}:ss'


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
    # o = OracleJob()
    # o.start_timer()
    n=arrow.now()
    a=n.shift(days=-1)
    x=a-n
    print(x.days)