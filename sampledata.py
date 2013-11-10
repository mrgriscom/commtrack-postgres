from datetime import datetime, timedelta
import random
from stocktest import conn, cur, submit_stock_report

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

def sample_data(root_location, num_reports, report_freq):
    """generate sample stock reports
    root_location -- parent of all locations for which to generate reports
    num_reports -- number of stock reports to generate per location
    report_freq -- mean interval between reports (days)
    """

    cur.execute("""
      select sms_code from descendants((select id from location where sms_code = %s)) d
      join location l on (l.id = d.id)
      where not exists (select null from location where parent = d.id);
    """, (root_location,))
    locations = [k['sms_code'] for k in cur.fetchall()]
    if not locations:
        locations = [root_location]

    cur.execute('select sms_code from product')
    products = [k['sms_code'] for k in cur.fetchall()]

    for loc in locations:
        sample_data_per_loc(loc, products, num_reports, report_freq)

    conn.commit()

def sample_data_per_loc(loc, products, num_reports, report_freq):
    delta = [random.random() for i in xrange(num_reports)]
    window = timedelta(days=num_reports * report_freq)
    report_timestamps = sorted(datetime.now() - timedelta(seconds=k * window.total_seconds()) for k in delta)

    def prod_stats():
        stock = random.uniform(0, 200)
        consumption = 1/random.uniform(1/3., 5.)
        return {'mean_stock': stock, 'mean_consumption': stock * consumption}
    prod_info = dict((p, prod_stats()) for p in products)

    def mk_frag(product, action, amount):
        code = {
            'stockonhand': 'soh',
            'stockout': 'so',
            'receipt': 'r',
            'consumption': 'c',
            'loss': 'l',
        }[action]

        if action == 'stockout':
            return '%s %s' % (product, code)
        else:
            return '%s %s %.1f' % (product, code, amount)
    def stock_report(timestamp, transactions):
        submit_stock_report(loc, [mk_frag(*tx) for tx in transactions], timestamp)
        print 'submitted stock report for %s at %s' % (loc, timestamp)

    stock = dict((k, v['mean_stock']) for k, v in prod_info.iteritems())
    
    # set initial levels
    last_report = datetime.now() - window
    stock_report(last_report, [(k, 'stockonhand', v) for k, v  in stock.iteritems()])

    for timestamp in report_timestamps:
        interval = timestamp - last_report
        frags = []
        for p, info in prod_info.iteritems():
            def tx(action, val=None):
                frags.append((p, action, val))

            expected_burn = interval.total_seconds() / 86400. / 30.44 * info['mean_consumption']
            receipts = random.uniform(0, 2 * expected_burn)
            consumption = random.uniform(0, 2 * expected_burn)

            # we track our own stock (which can go negative) independently of the soh in the
            # system so we can stay close to the chosen mean stock and simulate extended stockouts
            oldstock = stock[p]
            stock[p] = oldstock + receipts - consumption

            lossage = (random.random() if random.random() < .3 else 0) * consumption

            if oldstock <= 0 and stock[p] <= 0:
                # ongoing stockout
                if random.random() < .3:
                    tx('stockout')
                stock[p] *= (1 - abs(stock[p]) / info['mean_stock']) # nudge back towards restock
                continue
            if oldstock > 0 and stock[p] <= 0:
                # stockout!
                tx('stockout')
                continue
            if oldstock <= 0 and stock[p] > 0:
                # restocked!
                receipts -= abs(oldstock) # account for shortfall

            tx('receipt', receipts)
            if lossage:
                tx('loss', lossage)
            if random.random() > .4:
                tx('stockonhand', stock[p])
        stock_report(timestamp, frags)
        last_report = timestamp

def bootstrap():
    make_products()
    make_locations(6, 10) # 1e6 locs: takes ~6min on my machine
    sample_data('aaaa', 300, 7) # 100 locs, 5 yrs of weekly history: takes ~12min on my machine


