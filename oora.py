#!/usr/bin/python3
import cx_Oracle
import argparse
import sys
import os
import re
import csv
from prettytable import PrettyTable
from datetime import datetime

class Oora:
    def __init__(self):# {{{
        self.con=cx_Oracle.connect(os.environ['OORA_USER'], os.environ['OORA_PASS'], os.environ['OORA_HOST'])
        self.cur=self.con.cursor()
        self.delimiter=';'
        self.aligned=True
        self.csv_datefmt='%Y-%m-%d'
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
    def csv_values(self,query):# {{{
        '''
        Replace             aaa(city,year,mass,when)
        with    insert into aaa(city,year,mass,when) values(:1, :2, :3, :4)
        '''

        z=[ ":"+str(i[0]+1) for i in enumerate(query.split(',')) ]
        q="insert into {} values({})".format(query, ", ".join(z))
        return q
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
                    record.append(self.detect_type(val.strip()))
                collect.append(record)
        query=self.csv_values(query)
        self.cur.executemany(query, collect)
        self.con.commit()
        exit()
# }}}
    def detect_type(self,value):# {{{
        for typ,test in [ ('date', datetime.strptime),  ('int', int), ('float', float) ]:
            try:
                if typ == 'date':
                    return test(value, self.csv_datefmt)
                else:
                    return test(value)
            except ValueError:
                continue
        return value
# }}}
    def examples(self):# {{{
        print('''
oora -z
oora -c "drop table aaa" 
oora -c "create table aaa(city varchar(100), year integer, mass number, when date)" 
oora -t "aaa" 
oora -c "select * from aaa where rownum<=20 order by city" 
oora -c "delete from aaa where regexp_like (city,'War')" 
oora -c "insert into aaa(city,year) values('Warsaw', 2021)" 
oora -c "SELECT TABLE_NAME FROM all_tables where regexp_like(TABLE_NAME, '^DZ_') order by TABLE_NAME"
oora -c "SELECT TABLE_NAME FROM all_tables order by TABLE_NAME"
oora -c "begin PKG_ZMIANA_KLUCZY.KLUCZ_PRZEDMIOTU('BWbe-ND-C-BWUE','zupa'); end;"

==========================================

CSV:
oora -C /tmp/data.csv -c "aaa(city,year,mass,when)"
oora -C /tmp/data.csv -c "aaa(city,year,mass,when)" -d ';'
oora -C /tmp/data.csv -c "aaa(city,year,mass,when)" -D "%Y-%m-%d %H:%M:%S" 

Default CSV delimiter and date format. First row must be data, not header.
Warsaw    ; 1975 ; 1.0001 ; 2021-10-30
Berlin    ; 2021 ; 3.14   ; 2021-11-30
Amsterdam ; 2055 ; 4      ; 2021-12-30
''')
# }}}
    def argparse(self):# {{{
        parser = argparse.ArgumentParser(description="Oracle cmdline client")
        parser.add_argument('-c' , help='query'                , required=False)
        parser.add_argument('-d' , help='delimiter'            , required=False)
        parser.add_argument('-l' , help='list tables'          , required=False  , action='store_true')
        parser.add_argument('-t' , help='describe table'       , required=False)
        parser.add_argument('-u' , help='unaligned output'     , required=False  , action='store_true')
        parser.add_argument('-C' , help='csv import  (see -z)' , required=False)
        parser.add_argument('-D' , help='csv datefmt (see -z)' , required=False)
        parser.add_argument('-a' , help='find constraint'      , required=False)
        parser.add_argument('-z' , help='examples'             , required=False  , action='store_true')
        args = parser.parse_args()

        if args.d:
            self.delimiter=args.d
        if args.D:
            self.csv_datefmt=args.D
        if args.l:
            self.query("SELECT TABLE_NAME FROM all_tables order by TABLE_NAME")
        if args.a:
            self.query("select TABLE_NAME,COLUMN_NAME from user_cons_columns where lower(constraint_name) = lower('{}')".format(args.a))
        if args.t:
            self.query("SELECT COLUMN_NAME,DATA_TYPE,DATA_LENGTH from ALL_TAB_COLUMNS where lower(TABLE_NAME) = lower('{}')".format(args.t))
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

