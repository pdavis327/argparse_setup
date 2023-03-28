"""
Pipeline operational logic.  All the real work gets done here
"""

from util.bigquery import BigqueryHelper
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

class Pipeline:
    def __init__(self, args):
        self.log = logging.getLogger()
        self.args = args
    
    def square_num(self):
        num = self.args.input_num
        sq = num ** 2
        return sq

    def run(self):
        print(self.square_num())

