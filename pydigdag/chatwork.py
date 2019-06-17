# -*- coding: utf-8 -*-

import digdag
import os
import requests
import pprint
import pydigdag.control
from pydigdag.utils import _check_param_existence

class ChatworkApi(object):
    def __init__(self):
        self._check_digdag_params()
        self._chatwork_apikey = digdag.env.params['chatwork']['apikey']
        self._chatwork_endpoint = digdag.env.params['chatwork']['endpoint']
        self._chatwork_roomid = digdag.env.params['chatwork']['roomid']
        self._session_uuid = digdag.env.params["session_uuid"]
        self._session_time = digdag.env.params["session_time"]
        if not 'digdag' in digdag.env.params:
            self._digdag_endpoint = None
        else:
            if not 'endpoint' in digdag.env.params['digdag']:
                self._digdag_endpoint = None
            else:
                self._digdag_endpoint = digdag.env.params['digdag']['endpoint']

    def _check_digdag_params_chatwork(self, key):
        if not key in digdag.env.params['chatwork']:
            sys.stderr.write(self._alert_message_for_param('chatwork.{}'.format(key)))
            sys.exit(1)

    def _check_digdag_params(self):
        _check_param_existence(digdag.env.params, 'chatwork')
        self._check_digdag_params_chatwork('apikey')
        self._check_digdag_params_chatwork('endpoint')
        self._check_digdag_params_chatwork('roomid')

    def notify(self):
        # 失敗したタスク名を取得する
        cntl = pydigdag.control.control()
        task_name = cntl.get_failed_task()
        if task_name == "Unknown":
            url = "(コマンドラインからの実行のためURLはありません)"
        elif self._digdag_endpoint is None:
            url = "(digdag.dig が存在しないためURLはありません)"
        else:
            url = self._digdag_endpoint + 'sessions/' + str(digdag.env.params['session_id'])

        # Chatwork への投稿メッセージ作成
        post_message_url = '{}/rooms/{}/messages'.format(self._chatwork_endpoint, self._chatwork_roomid)
        body_message = u'以下のタスクでエラーが発生しました\n    タスク名: {}\n    セッションUUID：{}\n    セッション時間：{}\n    管理画面URL：{}'.format(task_name, self._session_uuid, self._session_time, url)

        # HTTPヘッダ作成
        headers = { 'X-ChatworkToken': self._chatwork_apikey }
        params = { 'body': body_message }

        # Chatwork へ投稿
        resp = requests.post(post_message_url, headers = headers, params = params)
        pprint.pprint(resp.content)
