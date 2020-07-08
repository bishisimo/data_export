"""
@author '彼时思默'
@time 2020/7/7 下午6:15
@describe: 
"""
import time
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from cassandra.cluster import Cluster
import pandas
import yaml
import arrow
from cassandra import ConsistencyLevel
from loguru import logger
import os
import warnings

warnings.filterwarnings("ignore")
pandas.set_option('display.max_columns', None)
pandas.set_option('display.width', 1000)
logger.add("./logger/error/error.log", rotation="1 year", enqueue=True, level='ERROR', delay=True)
logger.add("./logger/info/info.log", rotation="1 month", enqueue=True, level='INFO', delay=True)

class CassandraJob:
    def __init__(self):
        with open('config.yaml')as f:
            self.config = yaml.safe_load(f)
        self.cluster = Cluster(self.config['host'], connect_timeout=120, max_schema_agreement_wait=30)
        self.session = self.cluster.connect(self.config['db'])
        self.session.default_consistency_level = ConsistencyLevel.ONE
        self.session.default_timeout = None
        self.user_lookup_stmt = self.session.prepare(
            f'select * from {self.config["table"]} where ct>=? and ct<? ALLOW FILTERING')
        self._tmp_show_index = 0
        self.ok_dir = './ok/'
        self.err_dir = './err/'
        self.data_dir = f'/data/export/cassandra/{self.config["table"]}/'
        self._tmp_show_index=0
        self._init_dir()

    def _init_dir(self):
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)

    def record_err(self, d):
        """
        记录单日的失败数据
        :param f_n: 文件名
        :param v: vin
        :param i: 第几小时
        :return:
        """
        path = f'err.csv'
        if not os.path.exists(path):
            with open(path, 'w')as f:
                f.write("d\te\tt\n")
        with open(path, 'a')as f:
            f.write(f"{d}\t{arrow.now()}\n")


    def export(self,target_time:arrow.Arrow,deadline_time:arrow.Arrow):
        try:
            data=self.session.execute(self.user_lookup_stmt,[target_time.timestamp, deadline_time.timestamp])
            df=pandas.DataFrame(data)
        except:
            return None
        return df

    def export_monthly(self):
        target_month=arrow.now().floor('month')
        total_num=0
        while True:
            deadline_month=target_month
            target_month=target_month.shift(months=-1)
            df=self.export(target_month,deadline_month)
            if df is None:
                df=self.export_daily(target_month)
            df_len=len(df)
            if df_len==0:
                break
            total_num += df_len
            file_path = f"{self.data_dir}/"
            if not os.path.exists(file_path):
                os.makedirs(file_path)
            df.to_parquet(f"{file_path}{target_month.format('YYYY-MM')}.snappy.parquet", index=False)
            logger.info(f't={target_month},len={len(df)}')
        logger.info('all over!')

    def export_daily(self,target_month):
        month_days = list(target_month.range('day', target_month, target_month.shift(months=1)))
        target_time_bucket = []
        for i in range(len(month_days) - 1):
            target_time_bucket.append((month_days[i], month_days[i + 1]))
        # 查询数据
        df = pandas.DataFrame()
        for t_t, d_t in target_time_bucket:
            dd = self.export(t_t, d_t)
            if dd is None:
                dd = self.export_daily(t_t)
            df = df.append(dd)
        return df

    def export_hourly2day(self, target_day: arrow.Arrow):
        """
        将一天拆成小时查询并合并
        :param vins: 需要查询的vin
        :param target_day: 需要查询的精确到天的时间
        :return: 查询的vin的目标日的数据
        """
        day_hours = list(target_day.range('hour', target_day, target_day.shift(days=1)))
        target_time_bucket = []
        for i in range(len(day_hours) - 1):
            target_time_bucket.append((day_hours[i], day_hours[i + 1]))
        # 查询数据
        df = pandas.DataFrame()
        for t_t, d_t in target_time_bucket:
            dd, err_list = self.export(t_t, d_t)
            df = df.append(dd)
            if len(err_list)>0:
                self.record_err(t_t.format('YYY-MM-DD_HH'))
        return df