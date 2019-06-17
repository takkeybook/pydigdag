# -*- coding: utf-8 -*-

import digdag
import re
import copy
import subprocess
import sys
import os.path
import pydigdag.spinwait
import datetime
from pydigdag.utils import _check_param_existence

class control(object):
    def __init__(self, digdag = "/usr/local/bin/digdag"):
        self._digdag = digdag + " {COMMAND} {ARG}"

    def _convert_block_to_dict(self, cmd, arg):
        output = subprocess.Popen(self._digdag.format(COMMAND = cmd, ARG = arg), stdout = subprocess.PIPE, shell = True)
        block_list = []
        block = {}
        for line in iter(output.stdout.readline, ''):
            rec = re.split(": |:$", line.decode('UTF-8').strip())
            # 空行ならば終了
            if not line:
                break
            # COMMAND = "attempts" あるいは "schedules" の時に一行目を削除する
            if re.match("^Session attempts|^Schedules", rec[0]):
                continue

            if len(rec) > 1:
                # [key, value] という形の配列になっていれば辞書形式に変換
                block[rec[0]] = rec[1]
            else:
                # ブロックの最後は [''] となるのでまとめる作業をする
                # COMMAND = "tasks" の時には ['']→['n entries'] となるので、辞書が空でない場合のみ代入
                if len(block) > 0:
                    block_list.insert(len(block_list), copy.copy(block))
                    block = {}
        return(block_list)

    def _find_task_idx(self, tasks, task_name):
        idx = -1
        for n in range(len(tasks)):
            if re.match(".*\+{}$".format(task_name), tasks[n]["name"]):
                idx = n
                break

        if idx == -1:
            sys.stderr.write('タスク名 {} の task id が見つかりません\n'.format(task_name))
            sys.exit(1)

        return(idx)

    def _get_max_attempt_id_by_session_id(self):
        # 当該 session ID に対する attempt を全て取得する
        attempts = self._convert_block_to_dict("attempts", digdag.env.params["session_id"])

        # attempt ID が最大のものを取り出す
        max_attempt_id = -1
        for n in range(len(attempts)):
            if max_attempt_id < int(attempts[n]["attempt id"]):
                max_attempt_id = int(attempts[n]["attempt id"])

        # おそらく以下はありえないが念のためのチェックをする
        if max_attempt_id == -1:
            sys.stderr.write('当該 session ID に対する attempt がありません\n')
            sys.exit(1)

        return(max_attempt_id)

    def _get_max_attempt_id_by_session_time(self, project_name, workflow_name):
        # 指定プロジェクトのリストを取得
        projects = self._convert_block_to_dict("sessions", project_name)

        # 同じセッション時間で最大の attempt id のものを探す
        self_session_time = datetime.datetime.strptime(digdag.env.params["session_time"], '%Y-%m-%dT%H:%M:%S+09:00')
        attempt_id = -1
        for n in range(len(projects)):
            target_session_time = datetime.datetime.strptime(projects[n]["session time"], '%Y-%m-%d %H:%M:%S +0900')
            if projects[n]["workflow"] == workflow_name and target_session_time == self_session_time and attempt_id < int(projects[n]["attempt id"]):
                attempt_id = int(projects[n]["attempt id"])

        if attempt_id == -1:
            sys.stderr.write('指定されたプロジェクト/ワークフロー名に対する attempt がありません\n')
            sys.exit(1)

        return(attempt_id)

    def wait_task(self, **kwargs):
        _check_param_existence(kwargs, 'TASK_NAME')
        task_name = kwargs['TASK_NAME']
        _check_param_existence(kwargs, 'MAX_WAIT_TIME')
        max_wait_time = kwargs['MAX_WAIT_TIME']
        
        # SpinWait オブジェクトを生成する
        sp = pydigdag.spinwait.SpinWait()

        # attempt ID の最大値を取得する (attempt ID は単調増加であることを想定)
        max_attempt_id = self._get_max_attempt_id_by_session_id()

        # attempt ID が max_attempt_id の task リストを取り出し、指定したタスク名の task ID を取得する
        tasks = self._convert_block_to_dict("tasks", max_attempt_id)
        task_idx = self._find_task_idx(tasks, task_name)

        sys.stdout.write("TASK NAME: {}\n".format(task_name))
        sys.stdout.write("  TASK ID: {}\n".format(tasks[task_idx]["id"]))
        sys.stdout.write("   STATUS: {}\n".format(tasks[task_idx]["state"]))

        while tasks[task_idx]["state"] == "running":
            sp.wait()
            if sp.accumulate_wait_time > max_wait_time:
                sys.stderr.write("指定時間内に指定タスクが終了しませんでした\n")
                sys.exit(1)
            tasks = self._convert_block_to_dict("tasks", max_attempt_id)
            task_idx = self._find_task_idx(tasks, task_name)

        if tasks[task_idx]["state"] != "success":
            sys.stderr.write("待ち受けているジョブが正常終了しませんでした\n")
            sys.exit(1)

    def wait_local_file(self, **kwargs):
        _check_param_existence(kwargs, 'LOCAL_FILE')
        local_file = kwargs['LOCAL_FILE']
        _check_param_existence(kwargs, 'MAX_WAIT_TIME')
        max_wait_time = kwargs['MAX_WAIT_TIME']

        # SpinWait オブジェクトを生成する
        sp = pydigdag.spinwait.SpinWait()

        while not os.path.exists(local_file):
            sp.wait()
            if sp.accumulate_wait_time > max_wait_time:
                sys.stderr.write("指定時間内に指定ファイル {} が生成されませんでした\n".format(local_file))
                sys.exit(1)

    def _get_next_session_time(self, project, workflow):
        # スケジュールされたワークフローを取得
        schedules = self._convert_block_to_dict("schedules", "")

        # 指定ワークフローの次回実行のセッション時刻を取得
        next_session_time = ""
        for n in range(len(schedules)):
            if schedules[n]["project"] == project and schedules[n]["workflow"] == workflow:
                next_session_time = schedules[n]["next session time"]
        if next_session_time == "":
            sys.stderr.write('指定されたプロジェクト/ワークフローの実行スケジュールがありません\n')
            sys.exit(1)

        return(next_session_time)

    def _wait_scheduled_workflow(self, **kwargs):
        # 以下3変数は親メソッドで存在確認が済んでいることを前提とする
        project_name = kwargs['PROJECT_NAME']
        workflow_name = kwargs['WORKFLOW_NAME']
        max_wait_time = kwargs['MAX_WAIT_TIME']

        self_session_time = datetime.datetime.strptime(digdag.env.params["session_time"], '%Y-%m-%dT%H:%M:%S+09:00')
        target_next_session_time = datetime.datetime.strptime(self._get_next_session_time(project_name, workflow_name), '%Y-%m-%d %H:%M:%S +0900')

        if target_next_session_time < self_session_time:
            sys.stderr.write('指定されたプロジェクト/ワークフローの実行スケジュールが古すぎます\n')
            sys.exit(1)
        elif target_next_session_time > self_session_time:
            return(0)

        # SpinWait オブジェクトを生成する
        sp = pydigdag.spinwait.SpinWait()

        while self_session_time == target_next_session_time:
            sp.wait()
            if sp.accumulate_wait_time > max_wait_time:
                sys.stderr.write("指定時間内に指定ワークフローが開始しませんでした\n")
                sys.exit(1)
            target_next_session_time = datetime.datetime.strptime(self._get_next_session_time(project_name, workflow_name), '%Y-%m-%d %H:%M:%S +0900')

        # 待ち時間を返す
        return(sp.accumulate_wait_time)

    def wait_workflow(self, **kwargs):
        _check_param_existence(kwargs, 'PROJECT_NAME')
        project_name = kwargs['PROJECT_NAME']
        _check_param_existence(kwargs, 'WORKFLOW_NAME')
        workflow_name = kwargs['WORKFLOW_NAME']
        _check_param_existence(kwargs, 'MAX_WAIT_TIME')
        max_wait_time = kwargs['MAX_WAIT_TIME']

        # スケジュールされたワークフローを待つ
        wait_time = self._wait_scheduled_workflow(**kwargs)

        # 指定プロジェクトの最大 attempt id を取得する
        attempt_id = self._get_max_attempt_id_by_session_time(project_name, workflow_name)

        # attempt_id に該当する attempt 情報を取得する
        attempt = self._convert_block_to_dict("attempt", attempt_id)

        # SpinWait オブジェクトを生成する
        sp = pydigdag.spinwait.SpinWait()
        sp.accumulate_wait_time = wait_time

        while attempt[0]["status"] == "running":
            sp.wait()
            if sp.accumulate_wait_time > max_wait_time:
                sys.stderr.write("指定時間内に指定ワークフローが開始しませんでした\n")
                sys.exit(1)
            attempt = self._convert_block_to_dict("attempt", attempt_id)

        if attempt[0]["status"] != "success":
            sys.stderr.write("待ち受けているワークフローが正常終了しませんでした\n")
            sys.exit(1)

    def get_failed_task(self, **kwargs):
        # attempt ID の最大値を取得する (attempt ID は単調増加であることを想定)
        max_attempt_id = self._get_max_attempt_id_by_session_id()

        # attempt ID が max_attempt_id の task リストを取り出す
        tasks = self._convert_block_to_dict("tasks", max_attempt_id)

        # state が error のタスク名を取得
        task_name = "Unknown"
        for n in range(len(tasks)):
            if tasks[n]["state"] == "error":
                task_name = tasks[n]["name"]
                break

        return(task_name)
