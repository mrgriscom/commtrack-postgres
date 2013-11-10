from datetime import datetime, timedelta
import threading
import Queue
import time
import util as u

class ConsumptionUpdater(threading.Thread):
    def __init__(self, queue):
        super(ConsumptionUpdater, self).__init__()
        self.conn, self.cur = u.dbinit()
        self.queue = queue
        self.up = True

    def terminate(self):
        self.up = False

    def run(self):
        while self.up:
            try:
                timestamp, args = self.queue.get_nowait()
            except Queue.Empty:
                time.sleep(.01)
                continue
            while timestamp > datetime.now():
                time.sleep(.01)
            self.update_consumption(*args)

    def update_consumption(self, loc_id, product, timestamp):
        CONSUMPTION_WINDOW = 60 # days
        MIN_WINDOW = 10 # days
        MIN_PERIODS = 2

        self.cur.execute('select * from stockstate where (location, product, at_) = (%s, %s, %s)', (loc_id, product, timestamp))
        state = self.cur.fetchone()

        consumption = compute_consumption(self.cur, loc_id, product, timestamp, timedelta(days=CONSUMPTION_WINDOW),
                                          {'min_periods': MIN_PERIODS, 'min_window': MIN_WINDOW})
        self.cur.execute('update stockstate set consumption_rate = %s where id = %s', (consumption, state['id']))
        self.conn.commit()
        print 'updated consumption for %s %s @ %s' % (loc_id, product, timestamp)

def compute_consumption(cur, loc_id, product, window_end, window_size, params=None):
    window_start = window_end - window_size

    authoritative_actions = ['stockonhand', 'stockout']
    cur.execute('select * from consumption_transactions(%s, %s, %s, %s, %s)',
                (loc_id, product, window_start, window_end, authoritative_actions))
    transactions = cur.fetchall()

    return calc_consumption(transactions, window_start, window_end, params)

def calc_consumption(transactions, window_start, window_end, params=None):
    params = params or {}

    def span_days(start, end):
        return (end - start).total_seconds() / 86400.

    class ConsumptionPeriod(object):
        def __init__(self, tx):
            self.start = tx['at_']
            self.end = None
            self.consumption = 0

        def add(self, tx):
            self.consumption += tx['quantity']

        def close_out(self, tx):
            self.end = tx['at_']

        @property
        def length(self):
            return span_days(self.start, self.end)

        @property
        def normalized_length(self):
            return span_days(max(self.start, window_start), max(self.end, window_start))

        @property
        def normalized_consumption(self):
            return self.consumption * self.normalized_length / self.length

    def split_periods(transactions):
        period = None
        for tx in transactions:
            action = tx['action_']
            is_stockout = (
                action == 'stockout' or
                (action == 'stockonhand' and tx['quantity'] == 0)
            )
            is_checkpoint = (action == 'stockonhand' and not is_stockout)

            if is_checkpoint:
                if period:
                    period.close_out(tx)
                    yield period
                period = ConsumptionPeriod(tx)
            elif is_stockout:
                if period:
                    # throw out current period
                    period = None
            elif action == 'consumption':
                if period:
                    period.add(tx)
        if params.get('through_present', True) and period:
            period.close_out({'at_': window_end})
            yield period
    periods = list(split_periods(transactions))

    # exclude periods that occur entirely before the averaging window
    periods = filter(lambda period: period.normalized_length, periods)
    total_consumption = sum(period.normalized_consumption for period in periods) 
    total_length = sum(period.normalized_length for period in periods)

    # check minimum statistical significance thresholds
    if len(periods) < params.get('min_periods', 0) or total_length < params.get('min_window', 0):
        return None

    DAYS_IN_MONTH = 365.2425 / 12.
    return total_consumption / float(total_length) * DAYS_IN_MONTH if total_length else None

