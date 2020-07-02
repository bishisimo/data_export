"""
@author '彼时思默'
@time 2020/6/29 下午1:38
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
logger.remove()
logger.add("./logger/error/error.log",rotation="1 year", enqueue=True, level='ERROR', delay=True)
logger.add("./logger/info/info.log", rotation="1 month", enqueue=True, level='INFO', delay=True)

class CassandraJob:
    def __init__(self):
        with open('config.yaml')as f:
            self.config = yaml.safe_load(f)
        self.work_dir = f'/home/bishisimo/jupyter/data_export/cassandra/{self.config["table"]}'
        self.cluster = Cluster(self.config['host'], connect_timeout=120, max_schema_agreement_wait=30)
        self.session = self.cluster.connect(self.config['db'])
        self.session.default_consistency_level = ConsistencyLevel.ONE
        self.session.default_timeout = 30
        self.user_lookup_stmt = self.session.prepare(
            f'select * from {self.config["table"]} where v=? and ct>=? and ct<?')
        self.all_vins = pandas.DataFrame()  # 所有vin
        self.all_vins_len = 0  # 所有vin的书数量
        self._load_vins()
        self.now = arrow.now()
        self.t_year = self.now.year
        self.t_month = self.now.month
        self.t_day = self.now.day
        self._lock = False

    def _load_vins(self):
        sql = 'select v from t2_realtime_new'
        rows_v = self.session.execute(sql)
        all_vins = pandas.DataFrame(rows_v)
        self.all_vins = all_vins[all_vins.v.str.len() == 17].v.tolist()
        self.all_vins_len = len(all_vins)
        logger.info(f'all_vins_len={self.all_vins_len}')

    def _refresh_time(self, target_day):
        '''
        根据要导入的日期更新控制时间
        :param target_day: 要导入的时间
        :return:
        '''
        if target_day is None:
            target_day = arrow.now().floor('day').shift(days=-1)  # 昨天
        self.t_year = target_day.year
        self.t_month = target_day.month
        self.t_day = target_day.day

        self.target_timestamp = target_day.timestamp
        deadline = target_day.shift(days=1)
        self.deadline_timestamp = deadline.timestamp

    def _load_fp(self):
        ok_dir=f'{self.work_dir}/ok/'
        if not os.path.exists(self.ok_day_path):
            os.makedirs(self.ok_day_path)
        self.ok_day_path = f'{ok_dir}{self.t_year}_{self.t_month}_{self.t_day}.csv'
        err_dir=f'{self.work_dir}/err/'

        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
        self.err_day_path = f'{err_dir}day_{self.t_day}.csv'

        self.data_dir = f'/data/export/cassandra/{self.config["table"]}/{self.t_year}_{self.t_month}_{self.t_day}/'
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)

        with open(self.ok_day_path, 'w')as file:
            file.write("v\tn\tt\n")
        with open(self.err_day_path, 'w')as file:
            file.write("v\te\tt\n")

    def record_ok_day(self, v, n):
        """
        记录单日的成功数据
        :param v: vin
        :param n: len(vin)
        :return:
        """
        with open(self.ok_day_path, 'a')as file:
            file.write(f"{v}\t{n}\t{arrow.now()}\n")

    def record_err_day(self, i, v, e):
        """
        记录单日的失败数据
        :param i: 第几天
        :param v: vin
        :param e: 失败原因
        :return:
        """
        with open(self.err_day_path, 'a')as file:
            file.write(f"{i}\t{v}\t{e}\t{arrow.now()}\n")

    # 100查一次,3000为一个文件
    def export_by_day(self, target_day: arrow.Arrow = None):
        '''
        单日全部vi的数据导入
        :param target_day: 要导入的日期
        :return: None
        '''
        self._refresh_time(target_day)
        self._load_fp()

        batch_size = 100
        # batch_n = 30
        start_i = 0
        epoch = 0
        df = pandas.DataFrame()
        total_num = 0
        for batch_i in range((self.all_vins_len // batch_size) + 1):
            end_i = (batch_i + 1) * batch_size
            if end_i > self.all_vins_len:
                end_i = self.all_vins_len
            futures = []
            line_max = 100
            select_vins = self.all_vins[start_i:end_i]
            select_vins_len = len(select_vins)
            for i, v in enumerate(select_vins):
                futures.append(self.session.execute_async(self.user_lookup_stmt,
                                                          [v, self.target_timestamp, self.deadline_timestamp]))
                # ii = i * line_max // select_vins_len
                # process = round((i + 1) / select_vins_len * 100, 2)
                # print(f'\r创建进度：{"▉" * ii} i={i + 1},all={select_vins_len},process={process}%', end='')
            for i, future in enumerate(futures):
                try:
                    rows = future.result()
                    df = df.append(pandas.DataFrame(rows))
                    self.record_ok_day(self.all_vins[i], len(df))
                    # ii = i * line_max // select_vins_len
                    # process = round((i + 1) / select_vins_len * 100, 2)
                    # print(
                    #     f'\r完成进度：{"▉" * ii} i={i + 1},all={select_vins_len},process={process}%,本月进度:{round(end_i / self.all_vins_len * 100, 2)}%',
                    #     end='')
                except Exception as e:
                    self.record_err_day(i, select_vins[i], e)
            start_i = end_i
            df_len = len(df)
            if df_len >= 1200000 or end_i == self.all_vins_len:
                epoch += 1
                total_num += df_len
                df.to_parquet(f"{self.data_dir}/{epoch}.snappy.parquet", index=False)
                logger.info(f'completed:{self.t_year}_{self.t_month}_{self.t_day}{epoch},len={df_len}')
                df = pandas.DataFrame()
        logger.info(f'completed:{self.t_year}_{self.t_month}_{self.t_day}')
        return total_num

    @property
    def _is_rest(self) -> bool:
        """
        检查现在是不是休息状态,用来调度低优先级任务
        :return:
        """
        with open('config.yaml')as f:
            config = yaml.safe_load(f)
        now = arrow.now()
        start_sp = config['rest_time']['start'].split(':')
        start = now.replace(hour=int(start_sp[0]), minute=int(start_sp[1]))
        end_sp = config['rest_time']['end'].split(':')
        end = now.replace(hour=int(end_sp[0]), minute=int(end_sp[1]))
        if start < end and now > start and now < start or start > end and (now > start or now < end):  # 在指定休息时间
            return True
        if self._lock == False:
            return True
        return False

    def history_export(self):
        """
        历史数据导入,默认从昨日往前单日递推,或者从检测到日志最久的一天->往前
        :return:
        """
        history_path = 'history.csv'
        if not os.path.exists(history_path):
            target_day = self.now.floor('day')
            with open(history_path, 'w')as f:
                f.write(f't\tn')
        else:
            with open(history_path)as f:
                his = list(f.readlines())
                target_day = arrow.get(his[-1].split('\t')[0], tzinfo='local')
        while True:
            if self._is_rest:
                time.sleep(60)
                continue
            target_day = target_day.shift(days=-1)
            num = self.export_by_day(target_day)
            with open(history_path, 'a')as f:
                f.write(f'{target_day.format("YYYY-MM-DD")}\t{num}')
            if num == 0:
                return

    def latest_export(self):
        """
        历史数据导入,默认从昨日往前单日递推,或者从今天->检测到日志最新的一天
        :return:
        """
        history_path = 'history.csv'
        target_day = self.now.floor('day')
        if not os.path.exists(history_path):
            oldest_day = arrow.get('1970-01-01', tzinfo='local')
            with open(history_path, 'w')as f:
                f.write(f't\tn')
        else:
            with open(history_path)as f:
                his = list(f.readlines())
                oldest_day = arrow.get(his[-1].split('\t')[0], tzinfo='local')
        while True:
            if self._is_rest:
                time.sleep(60)
                continue
            target_day = target_day.shift(days=-1)
            if target_day == oldest_day:
                return
            num = self.export_by_day(target_day)
            with open(history_path, 'a')as f:
                f.write(f'{target_day.format("YYYY-MM-DD")}\t{num}')
            if num == 0:
                return

    def primary_run(self):
        """
        优先的任务进行加锁
        :return:
        """
        self._lock = True
        self.export_by_day()
        self._lock = False

    def start_timer(self, is_async=False):
        """
        开启定时任务
        :param is_async: 是否异步定时
        :param add_fun: 主线程执行的函数
        :return:
        """
        if is_async:
            scheduler = BackgroundScheduler()
        else:
            scheduler = BlockingScheduler()
        next_run_time = self.now.floor('day').shift(days=1)
        scheduler.add_job(self.primary_run, 'interval', days=1, next_run_time=next_run_time.naive)
        scheduler.start()



if __name__ == '__main__':
    c = CassandraJob()
    c.start_timer(True)
    c.history_export()
    while True:
        time.sleep(60)
