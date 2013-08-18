#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import gzip
import numpy as np

class PriceData(object):
    def __init__(self, filename):
        super(PriceData, self).__init__()

        lines = gzip.open(filename).read().splitlines()
        self._open = np.array([line.split(',')[2] for line in lines], np.float32)
        self._close = np.array([line.split(',')[5] for line in lines], np.float32)
        self._max = np.array([line.split(',')[3] for line in lines], np.float32)
        self._min = np.array([line.split(',')[4] for line in lines], np.float32)

        self._count = len(lines)
        self._start_time = datetime.datetime.strptime(
                        ''.join(lines[0].split(',')[:2]), '%Y%m%d%H%M%S')
        self._interval = datetime.timedelta(seconds=60)

    def time(self, i, count=1):
        ret = []
        for j in range(count):
            ret.append(self._start_time+(i-count+j+1)*self._interval)
        if count != 1:
            return ret
        else:
            return ret[0]

    def open(self, i, count=1):
        ret = self._open[i-count+1:i+1]
        if count != 1:
            return ret
        else:
            return ret[0]

    def close(self, i, count=1):
        ret = self._close[i-count+1:i+1]
        if count != 1:
            return ret
        else:
            return ret[0]

    def max(self, i, count=1):
        ret = self._max[i-count+1:i+1]
        if count != 1:
            return ret
        else:
            return ret[0]

    def min(self, i, count=1):
        ret = self._min[i-count+1:i+1]
        if count != 1:
            return ret
        else:
            return ret[0]

    def SMA(self, i, N, count=1):
        """ Simple Moving Average
            filter = [1/N, 1/N, ..., 1/N] """

        filter = np.ones(N)
        filter /= np.sum(filter)

        ret = np.convolve(filter, self.close(i-1, N+count-1), mode='valid')
        if count != 1:
            return ret
        else:
            return ret[0]

    def WMA(self, i, N, count=1):
        """ Weighted Moving Average
            filter \propto [N, N-1, ..., 1]
            norm(filter) = 1 """

        filter = np.array(range(N, 0, -1), np.float32)
        filter /= np.sum(filter)

        ret = np.convolve(filter, self.close(i-1, N+count-1), mode='valid')
        if count != 1:
            return ret
        else:
            return ret[0]

    def EMA(self, i, N, count=1):
        """ Exponential Moving Average
            filter \propto [1, 1-alpha, (1-alpha)^2, ...]
            alpha = 2/(N+1)
            norm(filter) = 1 """

        alpha =  2./(N+1)
        filter = np.zeros(N, np.float32)
        for j in range(N):
            filter[j] = (1-alpha)**j
        filter /= np.sum(filter) # normalize. not written in Wikipedia

        ret = np.convolve(filter, self.close(i-1, N+count-1), mode='valid')
        if count != 1:
            return ret
        else:
            return ret[0]

    def RMA(self, i, N, count=1):
        """ Modified Moving Average
            filter \propto [1, 1-alpha, (1-alpha)^2, ...]
            alpha = 1/N
            norm(filter) = 1 """
        alpha =  1./N
        filter = np.zeros(N, np.float32)
        for j in range(N):
            filter[j] = (1-alpha)**j
        filter *= alpha
        filter /= np.sum(filter) # normalize. not written in Wikipedia

        ret = np.convolve(filter, self.close(i-1, N+count-1), mode='valid')
        if count != 1:
            return ret
        else:
            return ret[0]

    def TMA(self, i, N, count=1):
        """ Triangular Moving Average
            filter \propto [1, 2, 3, ..., N-1, N, N-1, ..., 3, 2, 1]
            norm(filter) = 1 """

        filter = np.zeros(N*2-1)
        for j in range(N*2-1):
            filter[j] = N-abs(N-j-1)
        filter /= np.sum(filter)

        ret = np.convolve(filter, self.close(i-1, len(filter)+count-1), mode='valid')
        if count != 1:
            return ret
        else:
            return ret[0]

    def SWMA(self):
        """ Sine Weighted Moving Average
            details are not in Wikipedia """

        raise NotImplementedError

    def CA(self, i, count=1):
        """ Cumulative moving Average """

        ret = [np.mean(self.close(j, j+1)) for j in range(i-count, i)]

        if count != 1:
            return ret
        else:
            return ret[0]


if __name__ == '__main__':
    pd = PriceData('data/USDJPY_1min.txt.gz')
