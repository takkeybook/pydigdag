# -*- coding: utf-8 -*-

import sys
import re
import MySQLdb
import digdag
import pydigdag.spinwait
from pydigdag.utils import _check_param_existence

class MySQLJob(object):
    def __init__(self):
        self._check_digdag_params()
        self._connection = MySQLdb.connect(
            user = digdag.env.params['mysql']['user'],
            passwd = digdag.env.params['mysql']['password'],
            host = digdag.env.params['mysql']['host'],
            db = digdag.env.params['mysql']['database']
        )
        self._cursor = self._connection.cursor()
        self._connection.autocommit(False)

    def _check_digdag_params_mysql(self, key):
        if not key in digdag.env.params['mysql']:
            sys.stderr.write(self._alert_message_for_param('mysql.{}'.format(key)))
            sys.exit(1)

    def _check_digdag_params(self):
        _check_param_existence(digdag.env.params, 'mysql')
        self._check_digdag_params_mysql('user')
        self._check_digdag_params_mysql('password')
        self._check_digdag_params_mysql('host')
        self._check_digdag_params_mysql('database')

    def _exec_sql(self, sql):
        sys.stdout.write(sql + '\n')
        try:
            self._cursor.execute(sql)
            self._connection.commit()
        except Exception as e:
            self._connection.rollback()
            raise e
        finally:
            self._cursor.close()
            self._connection.close()

    def _prepare_sql(self, **kwargs):
        # クエリ定義ファイルを読み込む
        _check_param_existence(kwargs, 'QUERY_FILE')
        fd = open(kwargs['QUERY_FILE'])
        sql_base = fd.read()
        fd.close()

        # クエリ定義ファイルで用いられているパラメタが渡されているかをチェックする
        for var in re.findall(r'\{([A-Z_]+)\}', sql_base):
            _check_param_existence(kwargs, var)

        # パラメタを置換してクエリを完成させて返す
        return(sql_base.format(**kwargs))

    def partial_delete_hourly(self, **kwargs):
        _check_param_existence(kwargs, 'START_TIME')
        _check_param_existence(kwargs, 'OUTPUT')

        region_day = kwargs['START_TIME'][0:10]
        region_hour = int(kwargs['START_TIME'][11:13])
        db_name = kwargs['OUTPUT']
        sql = "DELETE FROM {} WHERE region_day = '{}' AND region_hour = {}".format(db_name, region_day, region_hour)

        self._exec_sql(sql)

    def partial_delete_daily(self, **kwargs):
        _check_param_existence(kwargs, 'START_DATE')
        _check_param_existence(kwargs, 'OUTPUT')

        sql = "DELETE FROM {OUTPUT} WHERE region_day = '{START_DATE}'".format(**kwargs)

        self._exec_sql(sql)

    def partial_delete_weekly(self, **kwargs):
        _check_param_existence(kwargs, 'START_WEEK_DATE')
        _check_param_existence(kwargs, 'OUTPUT')

        sql = "DELETE FROM {OUTPUT} WHERE region_week_start = '{START_WEEK_DATE}'".format(**kwargs)

        self._exec_sql(sql)

    def partial_delete_monthly(self, **kwargs):
        _check_param_existence(kwargs, 'START_MONTH')
        _check_param_existence(kwargs, 'OUTPUT')

        sql = "DELETE FROM {OUTPUT} WHERE region_month = '{START_MONTH}'".format(**kwargs)

        self._exec_sql(sql)

    def delete_all(self, **kwargs):
        _check_param_existence(kwargs, 'OUTPUT')

        sql = "DELETE FROM {OUTPUT}".format(**kwargs)

        self._exec_sql(sql)

    def run_sql(self, **kwargs):
        # クエリの準備をする
        sql = self._prepare_sql(**kwargs)

        # クエリを実行する
        self._exec_sql(sql)

    def wait(self, **kwargs):
        _check_param_existence(kwargs, 'MAX_WAIT_TIME')
        max_wait_time = kwargs['MAX_WAIT_TIME']

        # クエリの準備をする
        sql = self._prepare_sql(**kwargs)

        # SpinWait オブジェクトを生成する
        sp = pydigdag.spinwait.SpinWait()

        # autocommit の設定変更
        self._connection.autocommit(True)

        while self._cursor.execute(sql) < 1:
            sp.wait()
            if sp.accumulate_wait_time > max_wait_time:
                sys.stderr.write("指定時間内に指定テーブルに新規レコードが追加されませんでした\n")
                self._cursor.close()
                self._connection.close()
                sys.exit(1)

        self._cursor.close()
        self._connection.close()
