import psycopg2
from psycopg2.extras import DictCursor
import collections
from datetime import datetime, timedelta
import threading
import time
import util as u

def dbinit():
    conn = psycopg2.connect('dbname=stocktest')
    cur = conn.cursor(cursor_factory=DictCursor)
    return conn, cur
conn, cur = dbinit()

def make_locations(depth, fan, lineage=[], parent_id=None):
    """generate fake locations
    depth: how many levels of hierarchy
    fan: exponential growth factor
    """

    if not lineage:
        name = '_root'
        code = None
        loctype = '_root'
    else:
        code = ''.join(lineage)
        name = ':'.join(s.upper() for s in lineage)
        loctype = 'lvl%d' % len(lineage)

    cur.execute("""
      insert into location (name, parent, type, sms_code)
      values (%s, %s, %s, %s)
      returning id
    """, (name, parent_id, loctype, code))
    new_id = cur.fetchone()[0]
    print 'created %s [id %d]' % (name, new_id)

    if len(lineage) < depth:
        for i in range(fan):
            child = chr(ord('a') + i)
            make_locations(depth, fan, lineage + [child], new_id)

    if not parent_id:
        conn.commit()

def make_products():
    """create sample products"""

    PRODUCTS = [
        ('Condom', 'cm', 'Family Planning'),
        ('Coartem', 'co', 'Malaria'),
        ('Cotrimoxazole', 'cx', 'Antibiotic'),
        ('Rapid Diagnostic Test', 'rdt', 'HIV'),
        ('Sulfadoxine / Pyrimethamine', 'sp', 'Malaria'),
    ]

    for name, code, cat in PRODUCTS:
        cur.execute('insert into product (sms_code, name) values (%s, %s)', (code, name))
    conn.commit()

StockTx = collections.namedtuple('StockTx', ['product', 'action', 'subaction', 'quantity'])

def submit_stock_report(loc_code, data, timestamp=None):
    submission_id = u.mk_uuid()
    timestamp = timestamp or datetime.now()

    # look up location from sms code
    cur.execute('select id from location where sms_code = %s', (loc_code,))
    loc_id = cur.fetchone()[0]
    conn.commit()

    def mk_tx(arg):
        args = dict(zip(('product', 'action', 'quantity'), arg.split()))
        args['quantity'] = float(args['quantity']) if 'quantity' in args else None
        args['action'], args['subaction'] = {
            'r': ('receipt', None),
            'c': ('consumption', None),
            'soh': ('stockonhand', None),
            'so': ('stockout', None),
            'l': ('consumption', 'loss'),
        }[args['action']]
        return StockTx(**args)

    process_stock_report(loc_id, map(mk_tx, data), submission_id, timestamp)

def process_stock_report(loc_id, transactions, submission_id, timestamp):
    """process an incoming stock report
    loc_id: location id
    transactions: list of StockTx records
    submission_id: uuid of stock report submission
    timestamp: effective time of submission
    """
    # group transactions by product
    by_product = u.map_reduce(transactions, lambda e: [(e.product, e)])

    # process each product individually
    for product, transactions in by_product.iteritems():
        process_product_stock(loc_id, product, transactions, submission_id, timestamp)

def most_recent_state(cur, loc_id, product):
    cur.execute("""
      select * from stockstate where (location, product) = (%s, %s)
      order by at_ desc limit 1
      for update
    """, (loc_id, product))
    if cur.rowcount:
        return dict(cur.fetchone())

def process_product_stock(loc_id, product, transactions, submission_id, timestamp):
    # ensure transactions processed in correct order
    def tx_order(tx):
        return ['receipt', 'consumption', 'stockonhand', 'stockout'].index(tx.action)
    transactions.sort(key=tx_order)

    # get the current stock info for this product (and create row lock)
    state = most_recent_state(cur, loc_id, product)
    if state:
        del state['id']
    else:
        # first ever entry for this product at this loc
        state = {}

    # save a stock transaction to the database
    def commit_tx(tx):
        cur.execute("""
          insert into stocktransaction (at_, submission, location, product, action_, subaction, quantity)
          values (%s, %s, %s, %s, %s, %s, %s)
        """, (timestamp, submission_id, loc_id, product, tx.action, tx.subaction, tx.quantity))

    # save a 'fake' stock transaction if report stocked differs from what we've calculated it should be
    def reconcile(oldstock, newstock):
        diff = newstock - oldstock
        if diff != 0:
            reconciliation = StockTx(
                product,
                'consumption' if diff < 0 else 'receipt',
                '_initial' if state.get('last_reported') is None else '_inferred',
                abs(diff)
            )
            commit_tx(reconciliation)
        return newstock

    # process transactions and update stock (and other metadata)
    stock = state.get('current_stock', 0)
    for tx in transactions:
        if tx.action == 'receipt':
            stock += tx.quantity
        elif tx.action == 'consumption':
            stock -= tx.quantity
        elif tx.action in ('stockonhand', 'stockout'):
            newstock = 0 if tx.action == 'stockout' else tx.quantity
            stock = reconcile(stock, newstock)
            state['last_reported'] = timestamp
        commit_tx(tx)
    if stock < 0:
        stock = reconcile(stock, 0)
    state['stock_out_since'] = state.get('stock_out_since') or timestamp if stock == 0 else None

    state['current_stock'] = stock
    state['at_'] = timestamp
    state['location'] = loc_id
    state['product'] = product

    # commit updated state
    cur.execute('insert into stockstate (%s) values (%s)' % (', '.join(state.keys()), ', '.join(['%s'] * len(state))), state.values())
    conn.commit()

    threading.Thread(target=update_consumption, args=(loc_id, product)).start()

def update_consumption(loc_id, product):
    conn, cur = dbinit() # thread safety

    # simulate delay
    time.sleep(5.)

    CONSUMPTION_WINDOW = 60 # days
    MIN_WINDOW = 10 # days
    MIN_PERIODS = 2

    state = most_recent_state(cur, loc_id, product)
    consumption = compute_consumption(cur, loc_id, product, datetime.now(), timedelta(days=CONSUMPTION_WINDOW),
                                      {'min_periods': MIN_PERIODS, 'min_window': MIN_WINDOW})
    cur.execute('update stockstate set consumption_rate = %s where id = %s', (consumption, state['id']))
    conn.commit()
    conn.close()

def compute_consumption(cur, loc_id, product, window_end, window_size, params=None):
    window_start = window_end - window_size

    authoritative_actions = ['stockonhand', 'stockout']
    # TODO pass action types as postgres array to reduce risk of sql injection?
    cur.execute("""
      select * from stocktransaction
      where (location, product) = (%%(loc)s, %%(prod)s) and at_ between coalesce((
          -- get the date of the most recent 'full-count' stock transaction before the window start
          select max(at_) from stocktransaction 
          where (location, product) = (%%(loc)s, %%(prod)s) and at_ < %%(start)s
            and action_ in (%s)
        ), %%(start)s) and %%(end)s
      order by at_, id  -- id is importart to preserve intra-stockreport ordering
    """ % ', '.join("'%s'" % k for k in authoritative_actions),
                {'loc': loc_id, 'prod': product, 'start': window_start, 'end': window_end})
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









def test_stockreport():
    submit_stock_report('aaaaaa', (
            'co r 30',
            'cm r 20',
            'cx r 10',
            'co soh 65',
            'cm soh 40',
            'cx so',
    ))
