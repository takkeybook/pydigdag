# -*- coding: utf-8 -*-

from time import sleep

class SpinWait(object):
    __default_min_wait__ = 5      # 最小待ち時間 (単位：[s])
    __default_max_wait__ = 300    # 最大待ち時間 (単位：[s])

    def __init__(self, min_wait = __default_min_wait__, max_wait = __default_max_wait__):
        self._max_wait  = max_wait
        self._wait_time = min_wait
        self._accumulate_wait_time = 0

    @property
    def accumulate_wait_time(self):
        return(self._accumulate_wait_time)

    @accumulate_wait_time.setter
    def accumulate_wait_time(self, value):
        self._accumulate_wait_time = value

    def wait(self):
        print("waiting {} seconds".format(self._wait_time))
        sleep(self._wait_time)
        self._accumulate_wait_time += self._wait_time
        self._wait_time = min(self._max_wait, self._wait_time * 2)
