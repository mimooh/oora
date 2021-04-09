#!/usr/bin/python3
import cx_Oracle
import argparse
import sys
import os
import re
import csv
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
    def csv_import(self,f,query):# {{{
        ''' 
        We are guessing datatypes for arrays
        executemany("insert into aaa(city,year) values(:1, :2)", array(arrays))
        '''

        collect=[]
        with open(f, 'r') as file:
            reader = csv.reader(file, delimiter=self.delimiter)
            for row in reader:
                record=[]
                for val in row:
                    record.append(self.detect_type(val))
                collect.append(record)
        self.cur.executemany(query, collect)
        self.con.commit()
        exit()
# }}}
    def detect_type(self,value):# {{{
        for test in [ int, float ]:
            try:
                return test(value)
            except ValueError:
                continue
        return value
# }}}
    def examples(self):# {{{
        print('''
oora -c "create table aaa(city varchar(100), year integer)" 
oora -c "select * from aaa where rownum<=5" 
oora -c "select rownum,a.* from aaa a" 
oora -c "delete from aaa where regexp_like (city,'War')" 
oora -c "insert into aaa(city,year) values('Warsaw', 2021)" 
oora -c "SELECT TABLE_NAME FROM all_tables where TABLE_NAME like 'DZ_\%' order by TABLE_NAME"
oora -c "SELECT TABLE_NAME FROM all_tables order by TABLE_NAME"
oora -C /tmp/data.csv -c "insert into aaa(city,year) values(:1, :2)"
''')
# }}}
    def argparse(self):# {{{
        parser = argparse.ArgumentParser(description="Oracle cmdline client")
        parser.add_argument('-c' , help='query'               , required=False)
        parser.add_argument('-d' , help='delimiter'           , required=False)
        parser.add_argument('-u' , help='unaligned output'    , required=False  , action='store_true')
        parser.add_argument('-C' , help='csv import (see -z)' , required=False)
        parser.add_argument('-z' , help='examples'            , required=False  , action='store_true')
        args = parser.parse_args()

        if args.d:
            self.delimiter=args.d
        if args.C:
            self.csv_import(args.C, args.c)
        if args.u:
            self.aligned=False
        if args.c:
            self.query(args.c)
        if args.z:
            self.examples()

# }}}

Oora()

