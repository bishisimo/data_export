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
            f'select * from {self.config["table"]} where v=? and ct>=? and ct<?')
        self.all_vins = pandas.DataFrame()  # 所有vin
        self.all_vins_len = 0  # 所有vin的数量
        self._lock = False
        self._tmp_show_index = 0
        self.ok_dir = './ok/'
        self.err_dir = './err/'
        self.data_dir = f'/data/export/cassandra/{self.config["table"]}/'
        self._load_vins()
        self._init_dir()
        self.max_per_batch = 1000000
        self.batch_size = 10

    def _load_vins(self):
        """
        加载所有有效vin
        :return:
        """
        load_bk = False
        vin_record_path = './vin_record.csv'
        if load_bk and os.path.exists(vin_record_path):
            self.all_vins = pandas.read_csv(vin_record_path).v.tolist()
        else:
            sql = 'select v from t2_realtime_new'
            rows_v = self.session.execute(sql)
            all_vins = pandas.DataFrame(rows_v)
            all_vins = all_vins[all_vins.v.str.len() == 17]
            all_vins.to_csv(vin_record_path, index=False)
            self.all_vins = all_vins.v.tolist()
        self.all_vins_len = len(self.all_vins)
        logger.info(f'all_vins_len={self.all_vins_len}')

    def _init_dir(self):
        if not os.path.exists(self.ok_dir):
            os.makedirs(self.ok_dir)
        if not os.path.exists(self.err_dir):
            os.makedirs(self.err_dir)
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)


    def record_batch_ok(self, f_n:str, vs:list):
        """
        记录单日的成功数据
        :param f_n: 文件名
        :param vs: vins
        :return:
        """
        path = f'{self.ok_dir}{f_n}.csv'
        if not os.path.exists(path):
            with open(path, 'w')as f:
                f.write("v\tt\n")
        with open(path, 'a')as f:
            for v in vs:
                f.write(f"{v}\t{arrow.now()}\n")

    def record_one_err(self, f_n:str, v,i):
        """
        记录单日的失败数据
        :param f_n: 文件名
        :param v: vin
        :param i: 第几小时
        :return:
        """
        path = f'{self.err_dir}{f_n}.csv'
        if not os.path.exists(path):
            with open(path, 'w')as f:
                f.write("v\tn\tt\n")
        with open(path, 'a')as f:
            f.write(f"{v}\t{i}\t{arrow.now()}\n")

    def record_batch_err(self, f_n: str, vs:list,e:str=''):
        """
        记录单日的失败数据
        :param f_n: 文件名
        :param vs: vins
        :return:
        """
        path = f'{self.err_dir}{f_n}.csv'
        if not os.path.exists(path):
            with open(path, 'w')as f:
                f.write("v\te\tt\n")
        with open(path, 'a')as f:
            for v in vs:
                f.write(f"{v}\t{e}\t{arrow.now()}\n")

    def _show_process(self, t:arrow.Arrow,index: int,  df_len: int):
        line_max = 100
        # ii = index * line_max // batch_len
        ii = index * line_max // self.all_vins_len
        # process = round((index + 1) / batch_len * 100, 2)
        all_process = round(self._tmp_show_index / self.all_vins_len * 100, 4)
        print(
            f'\r完成进度：{"▉" * ii} i={index + 1},all={self.all_vins_len},time={t.format("YYYY-MM-DD")},data_len={df_len},单位时间进度:{all_process}%',
            end='')

    def export(self, vins: list, time_span: list):
        """
        导出数据的执行方法,控制数据量逻辑在外部实现;
        如果按月查询,则接收多个vin号同时查询结果;
        如果按天查询,则接受一个vin与多个time_span,并组合为一个月;
        如果按小时查询,则接受一个vin与time_span,组合为一天;
        :param vins:['aaa','bbb']
        :param time_span:[('2020-01-01 00:00:00','2020-01-02 00:00:00'),('2020-01-02 00:00:00','2020-01-03 00:00:00')]
        :return:
        """
        df = pandas.DataFrame()
        futures = []
        err_list = []
        for v in vins:
            for target_time, deadline_time in time_span:
                futures.append(self.session.execute_async(
                    self.user_lookup_stmt,
                    [v, target_time.timestamp, deadline_time.timestamp]))
        for i, future in enumerate(futures):
            try:
                rows = future.result()
                df = df.append(pandas.DataFrame(rows))
                # self._show_process(i, len(vins), len(df), len(err_list))
            except Exception as e:
                err_list.append(vins[i])
        return df, err_list

    def export_monthly(self, target_month: arrow.Arrow):
        """
        导入一个月的数据
        :param target_month: 要导入的月份
        :return:
        """
        batch_size = self.batch_size
        start_i = 0
        epoch = 0
        df = pandas.DataFrame()
        self._tmp_show_index=0
        total_num = 0
        for batch_i in range((self.all_vins_len // batch_size) + 1):
            # 选取新的vin号查询
            end_i = (batch_i + 1) * batch_size
            self._tmp_show_index=end_i
            if end_i > self.all_vins_len:
                end_i = self.all_vins_len
            select_vins = self.all_vins[start_i:end_i]
            # 月查询
            dd, err_list = self.export(select_vins, [(target_month, target_month.shift(months=1))])
            df = df.append(dd)
            # 错误vin号划分为日查询
            if len(err_list) > 0:
                df = df.append(self.export_daily2month(err_list, target_month))
            # 后续处理
            df_len = len(df)
            start_i = end_i
            if df_len >= self.max_per_batch or end_i == self.all_vins_len:
                epoch += 1
                total_num += df_len
                file_path=f"{self.data_dir}{target_month.format('YYYY-MM')}/"
                if not os.path.exists(file_path):
                    os.makedirs(file_path)
                df.to_parquet(f"{file_path}{epoch}.snappy.parquet", index=False)
                df = pandas.DataFrame()
                self._show_process(target_month,end_i, df_len)
                logger.info(f'completed:{target_month}_{epoch},len={df_len},,process={round(self._tmp_show_index / self.all_vins_len * 100, 4)}%')
                self.record_batch_ok(target_month.format('YYYY-MM'), select_vins)
        logger.info(f'completed:{target_month},total_num={total_num}')
        return total_num

    def export_daily2month(self, vins: list, target_month: arrow.Arrow):
        """
        将目标vin号的一个月拆分为单天查询后组合返回
        :param vins: 需要查询的vin
        :param target_month: 需要查询的精确到月的时间
        :return: 查询的vin的目标月的数据
        """
        # 拼装一个月的单日时间范围
        month_days = list(target_month.range('day', target_month, target_month.shift(months=1)))
        target_time_bucket = []
        for i in range(len(month_days) - 1):
            target_time_bucket.append((month_days[i], month_days[i + 1]))
        # 查询数据
        df = pandas.DataFrame()
        for t_t, d_t in target_time_bucket:
            dd, err_list = self.export(vins, [(t_t, d_t)])
            df = df.append(dd)
            if len(err_list) > 0:
                df = df.append(self.export_hourly2day(err_list, t_t))
        return df

    def export_hourly2day(self, vins: list, target_day: arrow.Arrow):
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
            dd, err_list = self.export(vins, [(t_t, d_t)])
            df = df.append(dd)
            if len(err_list)>0:
                for v in err_list:
                    self.record_one_err(target_day.format('YYY-MM-DD'),v,t_t)
        return df

    def export_one_day(self,target_day=None):
        """
        确保完整性的导入一天的数据
        :param target_day: 要导入的时间,默认为昨天
        :return: 当天的总数据量
        """
        if target_day is None:
            target_day = arrow.now()
        deadline_day = target_day.floor('day')
        target_day = deadline_day.shift(days=-1)
        t_t = (target_day, deadline_day)
        batch_size = self.batch_size
        start_i = 0
        epoch = 0
        df = pandas.DataFrame()
        total_num = 0
        self._tmp_show_index=0
        for batch_i in range((self.all_vins_len // batch_size) + 1):
            # 选取新的vin号查询
            end_i = (batch_i + 1) * batch_size
            self._tmp_show_index=end_i
            if end_i > self.all_vins_len:
                end_i = self.all_vins_len
            select_vins = self.all_vins[start_i:end_i]
            # 月查询
            dd, err_list = self.export(select_vins, [t_t])
            df = df.append(dd)
            # 错误vin号划分为日查询
            if len(err_list) > 0:
                df = df.append(self.export_hourly2day(err_list, target_day))
            # 后续处理
            df_len = len(df)
            start_i = end_i
            if df_len >= self.max_per_batch or end_i == self.all_vins_len:
                epoch += 1
                total_num += df_len
                file_dir=f"{self.data_dir}{target_day.format('YYYY-MM-DD')}/"
                if not os.path.exists(file_dir):
                    os.makedirs(file_dir)
                df.to_parquet(f'{file_dir}{epoch}.snappy.parquet', index=False)
                df = pandas.DataFrame()
                self._show_process(target_day,end_i,df_len)
                logger.info(f'completed:{target_day}_{epoch},len={df_len},process={round(self._tmp_show_index / self.all_vins_len * 100, 4)}%')
                self.record_batch_ok(target_day.format('YYYY-MM-DD'), select_vins)
        logger.info(f'completed:{target_day},total_num:{total_num}')
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
        return self._lock or end > start > now and now > start or start > end and (
                now > start or now < end)  # 在指定休息时间

    def history_export(self):
        """
        历史数据导入,默认从昨日往前单日递推,或者从检测到日志最久的一天->往前
        :return:
        """
        history_path = 'history_month.yaml'
        target_time = arrow.now().floor('month')
        his={}
        if os.path.exists(history_path):
            with open(history_path)as f:
                his:dict = yaml.safe_load(f)
                if len(his) > 0:
                    l=list(his.keys())
                    l.sort()
                    target_time = arrow.get(l[0], tzinfo='local')

        while True:
            if self._is_rest:
                time.sleep(60)
                continue
            target_time = target_time.shift(months=-1)
            num = self.export_monthly(target_time)
            if num == 0:
                return
            his[target_time.format("YYYY-MM")]=num
            with open(history_path, 'w')as f:
                yaml.safe_dump(his,f)

    def latest_export(self):
        """
        历史数据导入,默认从昨日往前单日递推,或者从今天->检测到日志最新的一天
        :return:
        """
        history_path = 'history_day.yaml'
        target_time = arrow.now().floor('day')
        oldest_day = arrow.get('2015-01-01', tzinfo='local')
        his={}
        if os.path.exists(history_path):
            with open(history_path)as f:
                his = yaml.safe_load(f)
                if len(his) > 0:
                    l = list(his.keys())
                    l.sort()
                    oldest_day = arrow.get(l[-1], tzinfo='local')
        while True:
            if self._is_rest:
                time.sleep(60)
                continue
            target_time = target_time.shift(days=-1)
            if target_time == oldest_day:
                return
            num = self.export_one_day(target_time)
            if num == 0:
                return
            his[target_time.format("YYYY-MM-DD")] = num
            with open(history_path, 'w')as f:
                yaml.safe_dump(his, f)


    def primary_run(self):
        """
        优先的任务进行加锁
        :return:
        """
        self._lock = True
        try:
            self.export_one_day()
        finally:
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
        next_run_time = arrow.now().floor('day').shift(days=1)
        scheduler.add_job(self.primary_run, 'interval', days=1, next_run_time=next_run_time.naive)
        scheduler.start()

    def export_by_v(self):
        deadline=arrow.now().floor('month')
        now=arrow.now()
        for i,v in enumerate(self.all_vins):
            try:
                data = self.session.execute(self.user_lookup_stmt, [v,0, deadline.timestamp])
            except Exception as e:
                self.record_batch_err('ve', [v],e)
                self._show_process(now, i, 0)
                continue
            df=pandas.DataFrame(data)
            file_dir = f"{self.data_dir}v/"
            if not os.path.exists(file_dir):
                os.makedirs(file_dir)
            df.to_parquet(f'{file_dir}{v}.snappy.parquet', index=False)
            self.record_batch_ok('vo',[v])
            self._tmp_show_index += 1
            self._show_process(now, i, len(df))


if __name__ == '__main__':
    # c = CassandraJob()
    # c.start_timer(True)
    # c.history_export()
    # while True:
    #     time.sleep(60)
    a= {'2020-01-01 01:02:01':10,'2020-01-01 01:01:01':10,'2020-02-01 01:01:01':20}
    with open('aa.yaml','w')as f:
        yaml.safe_dump(a,f)
    with open('aa.yaml')as f:
        b=yaml.safe_load(f)
    c=list(b.keys())
    c.sort()
    b={}
    print(c)
