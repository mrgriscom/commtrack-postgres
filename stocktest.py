import psycopg2
from psycopg2.extras import DictCursor
import collections
from datetime import datetime
import util as u

conn = psycopg2.connect('dbname=stocktest')
cur = conn.cursor(cursor_factory=DictCursor)

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

def process_product_stock(loc_id, product, transactions, submission_id, timestamp):
    # ensure transactions processed in correct order
    def tx_order(tx):
        return ['receipt', 'consumption', 'stockonhand', 'stockout'].index(tx.action)
    transactions.sort(key=tx_order)

    # get the current stock info for this product (and create row lock)
    cur.execute("""
      select * from stockstate where (location, product) = (%s, %s)
      order by at_ desc limit 1
      for update
    """, (loc_id, product))
    if cur.rowcount == 0:
        # first ever entry for this product at this loc
        state = {}
    else:
        state = dict(cur.fetchone())
        del state['id']

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


    ######
    # note: this should be done async
    #update_consumption()

    #conn.commit()

def update_consumption():
    pass





def test_stockreport():
    submit_stock_report('aaaaaa', (
            'co r 30',
            'cm r 20',
            'cx r 10',
            'co soh 65',
            'cm soh 40',
            'cx so',
    ))
