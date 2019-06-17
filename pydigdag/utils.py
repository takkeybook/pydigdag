# -*- coding: utf-8 -*-

import sys

def _check_param_existence(param_list, param_name):
    if not param_name in param_list:
        sys.stderr.write(_alert_message_for_param(param_name))
        sys.exit(1)

def _alert_message_for_param(parameter_name):
    return('_export ディレクティブで {} が設定されていません\n'.format(parameter_name))
