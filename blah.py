import psycopg2
import uuid

conn = psycopg2.connect('dbname=stocktest')
cur = conn.cursor()

def make_locations(depth, fan, lineage=[], parent_id=None):
    if not lineage:
        name = '_root'
        code = None
        loctype = '_root'
    else:
        code = ''.join(lineage)
        name = ':'.join(s.upper() for s in lineage)
        loctype = 'lvl%d' % len(lineage)

    cur.execute('insert into location (name, parent, type, sms_code) values (%s, %s, %s, %s) returning id', (name, parent_id, loctype, code))
    new_id = cur.fetchone()[0]
    print 'created %s [id %d]' % (name, new_id)

    if len(lineage) < depth:
        for i in range(fan):
            child = chr(ord('a') + i)
            make_locations(depth, fan, lineage + [child], new_id)

    if not parent_id:
        conn.commit()

def submit_stock_report(loc_code, tuples):
    pass

    pass

def compute_consumption():
    pass
