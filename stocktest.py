import collections
from datetime import datetime, timedelta
import Queue
import random
import util as u
from consumption import ConsumptionUpdater

conn, cur = u.dbinit()

changes_feed = Queue.PriorityQueue()
consumptionator = ConsumptionUpdater(changes_feed)
consumptionator.start()

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

    SIMULATED_DELAY = 0. #random.uniform(2, 8)
    changes_feed.put((datetime.now() + timedelta(seconds=SIMULATED_DELAY), (loc_id, product, timestamp)))

