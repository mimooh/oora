#!/usr/bin/python3
import cx_Oracle
import argparse
import sys
import os
import re
import csv
import json
from prettytable import PrettyTable
from datetime import datetime

class Oora:
    def __init__(self):# {{{
        self.con=cx_Oracle.connect(os.environ['OORA_USER'], os.environ['OORA_PASS'], os.environ['OORA_HOST'])
        self.cur=self.con.cursor()
        self.delimiter=';'
        self.aligned=False
        self.result_as_json=False
        self.csv_datefmt='%Y-%m-%d'
        self.argparse()
# }}}
    def query(self,query):# {{{
        if re.match("^\s*select", query, re.IGNORECASE):
            if self.result_as_json==True:
                self.as_json(query)
            elif self.aligned==True:
                self.aligned_select_query(query)
            else:
                self.nonaligned_select_query(query)
        else:
            self.cur.execute(query)
            self.con.commit()
# }}}
    def aligned_select_query(self,query):# {{{
        ''' Slow for large data sets '''

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
        data=[]
        for row in self.cur.execute(query):
            data.append(self.delimiter.join([ str(i) for i in row ]))
        header=self.delimiter.join([ str(i[0]) for i in self.cur.description ])
        print(header)
        print("\n".join(data))
# }}}
    def as_json(self,query):# {{{
        rows=[]
        for row in self.cur.execute(query):
            rows.append(row)
        header=[ str(i[0]) for i in self.cur.description ]

        out=[]
        for i in rows:
            out.append(dict(zip(header,i)))
        print(json.dumps(out, sort_keys=True, default=str))
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
oora -c "select * from aaa where rownum<=2 order by city" 
oora -c "delete from aaa where regexp_like (city,'War')" 
oora -c "insert into aaa(city,year) values('Warsaw', 2021)" 
oora -c "select object_name,procedure_name from user_procedures where regexp_like(object_name, 'ZMIANA')"
oora -c "begin PKG_ZMIANA_KLUCZY.KLUCZ_PRZEDMIOTU('BWbe-ND-C-BWUE','zupa'); end;"
oora -A "PRZCKL_PRZ_FK"

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
        parser.add_argument('-a' , help='aligned output'       , required=False  , action='store_true')
        parser.add_argument('-j' , help='as_json output'       , required=False  , action='store_true')
        parser.add_argument('-C' , help='csv import  (see -z)' , required=False)
        parser.add_argument('-D' , help='csv datefmt (see -z)' , required=False)
        parser.add_argument('-A' , help='find constraint'      , required=False)
        parser.add_argument('-z' , help='examples'             , required=False  , action='store_true')
        args = parser.parse_args()

        if args.d:
            self.delimiter=args.d
        if args.D:
            self.csv_datefmt=args.D
        if args.l:
            self.query("SELECT TABLE_NAME FROM all_tables order by TABLE_NAME")
        if args.A:
            self.query("select TABLE_NAME,COLUMN_NAME from user_cons_columns where lower(constraint_name) = lower('{}')".format(args.a))
        if args.t:
            self.query("SELECT COLUMN_NAME,NULLABLE,DATA_TYPE,DATA_LENGTH,DATA_DEFAULT from ALL_TAB_COLUMNS where lower(TABLE_NAME) = lower('{}') order by NULLABLE,COLUMN_NAME ".format(args.t))
        if args.C:
            self.csv_import(args.C, args.c)
        if args.a:
            self.aligned=True
        if args.j:
            self.result_as_json=1
        if args.c:
            self.query(args.c)
        if args.z:
            self.examples()

# }}}

Oora()

