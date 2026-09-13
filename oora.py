#!/usr/bin/python3
import oracledb as cx_Oracle
import argparse
import sys
import os
import re
import csv
from prettytable import PrettyTable
from datetime import datetime
from subprocess import Popen, PIPE

<<<<<<< Updated upstream
# cx_Oracle.init_oracle_client() # thick mode for old orace

class Oora:
    def __init__(self):# {{{
        #self.con=cx_Oracle.connect(os.environ['OORA_USER']      , os.environ['OORA_PASS']          , os.environ['OORA_HOST'])
=======
class Oora:
    def __init__(self):# {{{
>>>>>>> Stashed changes
        self.con=cx_Oracle.connect(user=os.environ['OORA_USER'] , password=os.environ['OORA_PASS'] , dsn=os.environ['OORA_HOST']+"/"+os.environ['OORA_SCHEMA'])
        self.cur=self.con.cursor()
        self.delimiter=';'
        self.csv_datefmt='%Y-%m-%d'
        self.argparse()
# }}}
    def query(self,query):# {{{
        if re.match(r"^\s*select", query, re.IGNORECASE):
            self.select_query(query)
        else:
            self.cur.execute(query)
            self.con.commit()
# }}}
    def select_query(self, query, params=()):  # {{{
        self.cur.execute(query, params)
        data = self.cur.fetchall()

        if not self.cur.description:
            return

        if getattr(self, "select_first_5", 0) == 1:
            data = data[:5]

        raw_headers = [str(col[0]).lower() for col in self.cur.description]
        formatted_rows = [ [str(val) if val is not None else "" for val in row] for row in data ]

        if len(data) <= 100:
            header = [ f"; {col}" if idx > 0 else col for idx, col in enumerate(raw_headers) ]
            x = PrettyTable()
            x.field_names = header
            x.align = 'l'
            x.border = False
            
            for row in formatted_rows:
                formatted_row = [ f"; {val}" if idx > 0 else val for idx, val in enumerate(row) ]
                x.add_row(formatted_row)
            print(x)
        else:
            header = self.delimiter.join(raw_headers)
            formatted_data = [ self.delimiter.join(row) for row in formatted_rows ]
            print(header)
            print("\n".join(formatted_data))
# }}}
    def csv_values(self,query): # {{{
        '''
        Replace             aaa(city,year,mass,when)
        with    insert into aaa(city,year,mass,when) values(:1, :2, :3, :4)
        '''

        z=[ ":"+str(i[0]+1) for i in enumerate(query.split(',')) ]
        q="insert into {} values({})".format(query, ", ".join(z))
        return q
# }}}
    def csv_datatypes(self,query):# {{{
        x=query.split("(")
        table=x[0]
        self.csv_header=[ i.strip() for i in x[1].upper().replace(')', '').split(',') ]
        self.types_conf={}
        query="SELECT COLUMN_NAME,DATA_TYPE from ALL_TAB_COLUMNS where lower(TABLE_NAME) = lower('{}')".format(table)
        for row in self.cur.execute(query):
            if row[1] in ['NUMBER']:
                self.types_conf[row[0]]=float
            elif row[1] in ['DATE']:
                self.types_conf[row[0]]=datetime.strptime
            else:
                self.types_conf[row[0]]=str
# }}}
    def csv_import(self,f,query):# {{{
        ''' 
        Preparing datatypes for the arrays
        executemany("insert into aaa(city,year) values(:1, :2)", array(arrays))
        '''

        self.csv_datatypes(query)
        collect=[]
        with open(f, 'r') as file:
            reader = csv.reader(file, delimiter=self.delimiter)
            for row in reader:
                collect.append(self.prepare_csv_record(zip(self.csv_header, [ i.strip() for i in row ])))
        query=self.csv_values(query)
        self.cur.executemany(query, collect)
        self.con.commit()
        exit()
# }}}
    def prepare_csv_record(self,record):# {{{
        xrecord=[]
        for a,val in record:
            try:
                if self.types_conf[a] == datetime.strptime:
                    xrecord.append(datetime.strptime(val, self.csv_datefmt))
                else:
                    xrecord.append(self.types_conf[a](val))
            except ValueError:
                xrecord.append(None)
        return xrecord
# }}}
    def examples(self):# {{{
        print('''
LIKE is not supported - use REGEXP_LIKE(attr, pattern)

oora -z
oora -c "drop table aaa" 
oora -c "create table aaa(city varchar(100), year integer, mass number, when date)" 
oora -t "aaa" 
oora -c "select * from aaa where rownum<=2 order by city" 
oora -c "delete from aaa where regexp_like (city,'warsa','i')" 
oora -c "insert into aaa(city,year) values('Warsaw', 2021)" 
oora -c "select object_name,procedure_name from user_procedures where regexp_like(object_name, 'ZMIANA')"
oora -c "begin PKG_ZMIANA_KLUCZY.KLUCZ_PRZEDMIOTU('BWbe-ND-C-BWUE','zupa'); end;"
oora -A "PRZCKL_PRZ_FK"
oora -f "script.sql"

================ CSV ================

Inserting from /tmp/data.csv: target db table must exist, no header in csv, ';' is delimiter, no newlines allowed:
Warsaw    ; 1975 ; 1.0001 ; 2021-10-30
Berlin    ; 2021 ; 3.14   ; 2021-11-30
Amsterdam ; 2055 ; 4      ; 2021-12-30

oora -C /tmp/data.csv -c "aaa(city,year,mass,when)"
oora -C /tmp/data.csv -c "aaa(city,year,mass,when)" -d ';'
oora -C /tmp/data.csv -c "aaa(city,year,mass,when)" -D "%Y-%m-%d %H:%M:%S" 

''')
# }}}
    def argparse(self):# {{{
        parser = argparse.ArgumentParser(description="Oracle cmdline client")
        parser.add_argument('-d' , help='delimiter'                              , required=False)
        parser.add_argument('-l' , help='list tables'                            , required=False  , action='store_true')
        parser.add_argument('-f' , help='run a script in sqlplus'                , required=False)
        parser.add_argument('-t' , help='describe table'                         , required=False)
        parser.add_argument('-L' , help='select: first 5 results'                , required=False  , action='store_true')
        parser.add_argument('-D' , help='csv datefmt (see -z)'                   , required=False)
        parser.add_argument('-c' , help='query'                                  , required=False)
        parser.add_argument('-C' , help='csv import  (see -z)'                   , required=False)
        parser.add_argument('-A' , help='describe constraint / trigger'          , required=False)
        parser.add_argument('-z' , help='examples'                               , required=False  , action='store_true')
        args = parser.parse_args()

        if args.d:
            self.delimiter=args.d
        if args.D:
            self.csv_datefmt=args.D
        if args.l:
            self.query("SELECT TABLE_NAME FROM all_tables order by TABLE_NAME")
            self.query("select sys_context('USERENV','SERVER_HOST') as host from dual")
        if args.f:
            self.run_sql_script(args.f)
        if args.A:
            self.query("select TABLE_NAME,COLUMN_NAME from user_cons_columns where lower(constraint_name) = lower('{}')".format(args.A))
            self.query("select TRIGGER_BODY           from all_triggers where lower(trigger_name) = lower('{}')".format(args.A))
        if args.t:
            self.query("SELECT COLUMN_NAME,NULLABLE,DATA_TYPE,DATA_LENGTH,DATA_DEFAULT from ALL_TAB_COLUMNS where lower(TABLE_NAME) = lower('{}') order by NULLABLE,COLUMN_NAME ".format(args.t))
        if args.C:
            self.csv_import(args.C, args.c)
        if args.L:
            self.select_first_5=1
        if args.c:
            self.query(args.c)
        if args.z:
            self.examples()

# }}}
    def run_sql_script(self, filename):# {{{
        ''' We are running database as user oracle, right? '''

        os.system("exit | sudo -i -u oracle sqlplus -S \"{}/{}\" @{}".format(os.environ['OORA_USER'], os.environ['OORA_PASS'], filename))
# }}}

Oora()
