from datetime import datetime
from datetime import date


def as_date_str(d):
    result = d
    if type(d) in [datetime, date]:
        result = d.strftime('%Y-%m-%d')
    return result
