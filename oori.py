#!/usr/bin/python3
import cx_Oracle
import argparse
import sys
import os

class Oracle:
    def __init__(self):# {{{
        self.con=cx_Oracle.connect(user=os.environ['ORACLE_USER'], password=os.environ['ORACLE_PASS'])
        self.cur=self.con.cursor()
        self.delimiter=';'
        self._argparse()
# }}}

    def query(self,q):# {{{
        data=''
        print(11)
        if query.startswith('select') or query.startswith('SELECT'):
            for row in cursor.execute(query):
                data+=";".join([ str(i) for i in row ])+"\n"
            print(";".join([ str(i[0]) for i in cursor.description ]))
            print(data)
        else:
            cursor.execute(query)
            self.con.commit()
# }}}
    def _argparse(self):# {{{
        parser = argparse.ArgumentParser(description="Oracle cmdline client ")
        parser.add_argument('-q' , help='query'     , required=True  , action='store_true')
        parser.add_argument('-d' , help='delimiter' , required=False , action='store_true')
        args = parser.parse_args()

        if args.d:
            self.delimiter=arg.d
        if args.q:
            self.query(args.q)

# }}}

Oracle()
