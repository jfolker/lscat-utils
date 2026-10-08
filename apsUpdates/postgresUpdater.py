import json
import traceback
import types
import sys
from datetime import *
from enum import Enum

import psycopg2

import lslog

def parsePgTimestamp(tsStr):
    return datetime.strptime(tsStr, '%m/%d/%Y:%H:%M:%S')


class esafModification(Enum):
    UPDATE = 1  # An update to an existing experiment
    INSERT = 2  # A new experiment
    NONE = 3  # Our record is older than what's currently in postgres


class transactionState(Enum):
    AWAIT_BEGIN = 1
    AWAIT_END = 2
    AWAIT_COMMIT = 3


class postgresUpdater:
    dryRun = True
    state = transactionState.AWAIT_BEGIN  # See enum definition above.
    pgConnection = None  # The DB connection.
    pgCursor = None  # The psycopg2 "cursor" used to execute commands.

    def __init__(self, dryRun=False):
        self.dryRun = dryRun
        self.state = transactionState.AWAIT_BEGIN

        # Setup postgres session
        self.pgConnection = psycopg2.connect(user='postgres',
                                             dbname='esaf')
        self.pgCursor = self.pgConnection.cursor()
    
    def __command(self, command, userInput=None):
        # In a dry run, the command will be automatically rolled back later.
        # We log the command that actually gets ran before running it.
        if userInput and len(userInput) > 0:
            queryStr = self.pgCursor.mogrify(command, userInput)
            lslog.info(f'SQL query - "{queryStr}"')
            self.pgCursor.execute(command, userInput)
        else:
            lslog.info(f'SQL query - "{command}"')
            self.pgCursor.execute(command)

    def __select(self, table, columns=None, filterDict=None):
        if not table:
            raise ValueError('Illegal SELECT query, no table name')

        # If no columns were specified, this is a "SELECT * FROM {table}"
        colFormat = "*"
        if columns and len(columns) > 0:
            colFormat = ""
            needSeparator = False
            for column in columns:
                if needSeparator:
                    colFormat += f",{column}"
                else:
                    colFormat += f"{column}"
                    needSeparator = True

        # If we are not filtering for specific values, we have our full query.
        if filterDict is None or len(filterDict) <= 0:
            queryTemplate = f'SELECT {colFormat} FROM {table};'
            self.__command(queryTemplate)
            return self.pgCursor.fetchall()
        else:
            whereFormat = ""
            needSeparator = False
            for k, v in filterDict.items():
                if needSeparator:
                    whereFormat += f" AND {k}=%s"
                else:
                    whereFormat += f"{k}=%s"
                    needSeparator = True

            queryTemplate = f'SELECT {colFormat} FROM {table} WHERE {whereFormat};'
            self.__command(queryTemplate, tuple(filterDict.values()))
            return self.pgCursor.fetchall()

    def __delete(self, table, filterDict=None):
        if not table:
            raise ValueError('Illegal DELETE query, no table name')

        if filterDict is None or len(filterDict) <= 0:
            queryTemplate = f"DELETE FROM {table};"
            self.__command(queryTemplate)
            return

        queryTemplate = f"DELETE FROM {table} WHERE "
        needSeparator = False
        for k, v in filterDict.items():
            if needSeparator:
                queryTemplate += f" AND {k}=%s"
            else:
                queryTemplate += f"{k}=%s"
                needSeparator = True

        queryTemplate += ";"
        self.__command(queryTemplate, tuple(filterDict.values()))
        
    # Does an SQL insert with data santized to prevent injection attacks.
    def __insert(self, table, dataDict):
        if not table:
            raise ValueError('Illegal INSERT query, no table name')
        elif dataDict is None or len(dataDict) <= 0:
            raise ValueError('Illegal INSERT query, no values')
        
        # Prepend a comma before every item added except the first.
        colFormat = ""
        valFormat = ""
        needSeparator = False
        for k, v in dataDict.items():
            if needSeparator:
                colFormat += f", {k}"
                valFormat += ", %s"
            else:
                colFormat += f"{k}"
                valFormat += "%s"
                needSeparator = True

        queryTemplate = f'INSERT INTO {table} ({colFormat}) VALUES ({valFormat});'
        self.__command(queryTemplate, tuple(dataDict.values()))

    # Does an SQL update with data santized to prevent injection attacks.
    def __update(self, table, dataDict, filterDict=None):
        if not table:
            raise ValueError('Illegal UPDATE query, no table name')
        elif dataDict is None or len(dataDict) <= 0:
            raise ValueError('Illegal UPDATE query, no values')
        elif filterDict is None or len(filterDict) <= 0:
            # An UPDATE without a WHERE clause is legal, but almost
            # never a good thing.
            raise ValueError('Suspicious UPDATE query, no WHERE criteria.')
        
        # Prepend a comma before every item added except the first.
        queryTemplate = f'UPDATE {table} SET '
        needSeparator = False
        for k, v in dataDict.items():
            if needSeparator:
                queryTemplate += f", {k}=%s"
            else:
                queryTemplate += f"{k}=%s"
                needSeparator = True

        if not filterDict or len(filterDict) <= 0:
            userInput = tuple(dataDict.values())
            self.__command(queryTemplate, userInput)
            return
        
        queryTemplate += " WHERE "
        needSeparator = False
        for k, v in filterDict.items():
            if needSeparator:
                queryTemplate += f" AND {k}=%s"
            else:
                queryTemplate += f"{k}=%s"
                needSeparator = True

        queryTemplate += ';'
        userInput = tuple(dataDict.values()) + tuple(filterDict.values())
        self.__command(queryTemplate, userInput)

    def __beginTransaction(self):
        if not self.state == transactionState.AWAIT_BEGIN:
            raise RuntimeError('Attempted to BEGIN a transaction prior to END or COMMIT of another transaction.')

        self.state = transactionState.AWAIT_END
        self.__command("begin")
        self.__command("SET CONSTRAINTS ALL DEFERRED")

    def __endTransaction(self):
        if not self.state == transactionState.AWAIT_END:
            raise RuntimeError(
                'Attempted to END a transaction after COMMIT or before BEGIN')

        self.pgCursor.execute("end")
        self.state = transactionState.AWAIT_COMMIT

    def __commitTransaction(self):
        if not self.state == transactionState.AWAIT_COMMIT:
            raise RuntimeError('Attempted to COMMIT before transaction END')

        if self.dryRun:
            lslog.info('Dry Run mode is enabled, doing an SQL rollback instead of commit')
            self.__rollbackTransaction()
        else:
            self.__command("commit")

        self.state = transactionState.AWAIT_BEGIN

    def __rollbackTransaction(self):
        if not self.state == transactionState.AWAIT_COMMIT:
            raise RuntimeError('Attempted to COMMIT before transaction END')

        self.__command("rollback")
        self.state = transactionState.AWAIT_BEGIN

    # Parses all the tag-value pairs in the top level of dom and puts the
    # values into a dictionary indexed by their postgres column name using
    # nameDict to convert the XML tag name to the name of the postgres row.
    #
    # NOTE: All santizing of data is, and should continue to be done here
    # and only here.
    def processDocument(self, dom):
        result = {}
        users = {}
        columns = {}
        for child in dom.iter(tag='experimenter'):
            is_pi = False
            for c in child.iter(tag='exp_spokesperson'):
                if c.text and c.text == 'Y':
                    is_pi = True
            
            badgeno = None
            for c in child.iter(tag='exp_badge_no'):
                if not c.text:
                    continue
                badgeno = int(c.text)
            
            email = 'fixme@ls-cat.org'
            for c in child.iter(tag='exp_email'):
                if not c.text:
                    continue
                email = c.text.strip().replace('\'', '')

            firstname = None
            for c in child.iter(tag='exp_fn'):
                if not c.text:
                    continue
                firstname = c.text.strip().replace('\'', '')
            
            lastname = None
            for c in child.iter(tag='exp_ln'):
                if not c.text:
                    continue
                lastname = c.text.strip().replace('\'', '')

            if firstname and lastname and email:
                users[badgeno] = (firstname, lastname, email)
            else:
                lslog.notice(f"One or more fields is missing for a user: badgeno={badgeno},firstname={firstname},lastname={lastname},email={email}")
                
            if is_pi:
                columns['pibadgeno'] = badgeno
                columns['piemail'] = email
        
        esafId = None
        for child in dom.iter(tag='*'):
            if not child.text:
                continue
            
            # Remove trailing/leading whitespace commonly found in
            # description and comment fields, and escape single quotes the
            # right way.
            sanitizedValue = child.text.strip()
            if child.tag == 'experiment_id':
                esafId = int(sanitizedValue)
            elif child.tag == 'experiment_title':
                columns['title'] = sanitizedValue
            elif child.tag == 'id_start_date' or child.tag == 'bm_start_date':
                columns['startdate'] = sanitizedValue
            elif child.tag == 'id_end_date' or child.tag == 'bm_end_date':
                columns['enddate'] = sanitizedValue
            elif child.tag == 'timestamp':
                columns['lastupdatedtime'] = parsePgTimestamp(sanitizedValue).replace(tzinfo=None)
            elif child.tag == 'proprietary_flag':
                columns['proprietary'] = sanitizedValue	== 'Y'
            elif child.tag == 'classified_flag':
                columns['classified']  = sanitizedValue == 'Y'
        
        queryResults = self.__select('public.experiments', ['lastupdatedtime'], {'esafid': esafId})
        if len(queryResults) <= 0:
            columns['esafid'] = esafId
            self.__beginTransaction()
            self.__insert('public.experiments', columns)
            self.__endTransaction()
            self.__commitTransaction()
           
        else:
            dbTimestamp = None
            if queryResults[0][0] is not None:
                dbTimestamp = queryResults[0][0].replace(tzinfo=None)
            
            if dbTimestamp is None or dbTimestamp <= columns['lastupdatedtime']:
                self.__beginTransaction()
                self.__update('public.experiments', columns, {'esafid': esafId})
                self.__endTransaction()
                self.__commitTransaction()
        
        result[esafId] = users
        return result
