from datetime import datetime
import time

@outputSchema('unixtime:float')
def convert_date(date):
    splittime = [int(x) for x in date.split("-")]
    dt = datetime(splittime[0], splittime[1], splittime[2])
    unixtime = time.mktime(dt.timetuple())
    return float(unixtime)

