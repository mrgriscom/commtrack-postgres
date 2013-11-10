requirements
============
psycopg2

setup
=====

    $ createdb stocktest
    $ psql stocktest < schema.sql

    $ python
    > import sampledata
    > sampledata.bootstrap() # will take ~20 minutes

using
=====

submit a stock report like:

    # 'product action quantity'
    stocktest.submit_stock_report(<loc_id>, ['cx soh 40', 'cx r 17', 'cm so', 'co l 6']) 

play with reports in `psql`:

`current_state` view shows current stock for all products/locations

the `*_report` functions mimic the current commtrack 'Inventory', 'Reporting Rate', and 'Stock Status by Product' reports, respectively.