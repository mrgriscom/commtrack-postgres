import psycopg2
from psycopg2.extras import DictCursor
import uuid
import collections
import settings

def dbinit():
    conn = psycopg2.connect('dbname=%s' % settings.DB)
    cur = conn.cursor(cursor_factory=DictCursor)
    return conn, cur

def mk_uuid():
    return uuid.uuid4().hex[:12]

def map_reduce(data, emitfunc=lambda rec: [(rec,)], reducefunc=lambda v: v):
    """perform a "map-reduce" on the data

    emitfunc(datum): return an iterable of key-value pairings as (key, value). alternatively, may
        simply emit (key,) (useful for reducefunc=len)
    reducefunc(values): applied to each list of values with the same key; defaults to just
        returning the list
    data: iterable of data to operate on
    """
    mapped = collections.defaultdict(list)
    for rec in data:
        for emission in emitfunc(rec):
            try:
                k, v = emission
            except ValueError:
                k, v = emission[0], None
            mapped[k].append(v)
    return dict((k, reducefunc(v)) for k, v in mapped.iteritems())
