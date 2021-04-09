#!/usr/bin/python3
import cx_Oracle
import argparse
import sys
import os
import re
from prettytable import PrettyTable

class Oora:
    def __init__(self):# {{{
        self.con=cx_Oracle.connect(os.environ['OORA_USER'], os.environ['OORA_PASS'], os.environ['OORA_HOST'])
        self.cur=self.con.cursor()
        self.delimiter=';'
        self.aligned=True
        self.argparse()
# }}}
    def query(self,query):# {{{
        if re.match("^\s*select", query, re.IGNORECASE):
            if self.aligned==True:
                self.aligned_select_query(query)
            else:
                self.nonaligned_select_query(query)
        else:
            self.cur.execute(query)
            self.con.commit()
# }}}
    def aligned_select_query(self,query):# {{{
        data=[]
        for row in self.cur.execute(query):
            data.append([ "; "+str(i) for i in row ])
        header=[ "; "+str(i[0]) for i in self.cur.description ]

        x = PrettyTable()
        x.field_names = header
        x.align='l'
        x.border=False
        for i in data:
            x.add_row(i)
        print(x)
# }}}
    def nonaligned_select_query(self,query):# {{{
        data=''
        for row in self.cur.execute(query):
            data+=self.delimiter.join([ str(i) for i in row ])+"\n"
        header=self.delimiter.join([ str(i[0]) for i in self.cur.description ])
        print(header)
        print(data)
# }}}
    def argparse(self):# {{{
        parser = argparse.ArgumentParser(description="Oracle cmdline client")
        parser.add_argument('-c' , help='query'            , required=False)
        parser.add_argument('-d' , help='delimiter'        , required=False)
        parser.add_argument('-u' , help='unaligned output' , required=False  , action='store_true')
        args = parser.parse_args()

        if args.d:
            self.delimiter=args.d
        if args.u:
            self.aligned=False
        if args.c:
            self.query(args.c)

# }}}

Oora()

