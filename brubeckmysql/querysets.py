#!/usr/bin/env python
# Copyright 2012 Brooklyn Code Incorporated. See LICENSE.md for usage
# the license can also be found at http://brooklyncode.com/opensource/LICENSE.md
import json
import logging
import os
import imp

import pymysql
from pymysql.connections import Connection
from pymysql import cursors
from brubeck.queryset import AbstractQueryset
from dictshield.fields import mongo as MongoFields
from dictshield.fields import compound as CompoundFields
from base import create_db_conn_pool
from base import create_db_conn
import dictshield

from gevent.queue import Queue

###
### All of our data interaction with any data store happens in a Queryset object
###

class MySqlQueryset(object):
    """base class mixin for MySql backed Queryset objects"""

    # Used to set the format of our returned object
    # FORMAT_DICT is a reasonable default since Brubeck
    # uses that to create both DictShield objects and JSON responses
    
    FORMAT_TUPLE = 0
    FORMAT_DICT  = 1
    FORMAT_DICTSHIELD  = 2

    def __init__(self, settings, db_conn, **kw):
        """load our settings and do minimal config"""
        self.settings = settings
        if isinstance(db_conn, Queue):
            self.db_pool = db_conn
            self.db_conn = None
        else:
            self.db_conn = db_conn
            self.db_pool = None


        if self.db_pool == None and self.db_conn == None:
            logging.debug("None db_conn passed to queryset __init__t")

        # We will need to set these in the entity specific implmentation
        # Once DictShield has more meta data, this may not be necessary anymore        
        self.table_name = None          # the name of the database table
        self.fields = None              # A list of field names

    def set_db_pool(self, db_pool):
        """set our db_pool (gevent.queue.Queue)"""
        self.db_pool = db_pool

    def set_db_conn(self, db_conn):
        """set our db_conn"""
        self.db_conn = db_conn

    def get_db_pool(self):
        """get our db_pool (gevent.queue.Queue)"""
        return self.db_pool

    def get_db_conn(self):
        """Make sure we have a db connection, and return it"""
        db_conn = None
        if self.db_conn == None and self.db_pool == None:
            self.init_db_conn()
        # get a connection        
        if not self.db_pool == None:
            # Will block until one becomes available
            db_conn = self.db_pool.get()
        elif not self.db_conn == None:
            # not using pooling
            db_conn = self.db_conn

        if not db_conn == None:
            # try to avoid broken pipe error
            try:
                db_conn.ping()
            except:
                # if we have any problems just give us a fresh connection
                logging.debug("Error pinging, building new connection")
                # first kill our old connection
                if isinstance(db_conn, Connection):
                    try:
                        db_conn.close()
                    except:
                        pass
                db_conn = None
                try:
                    ## create our mySql connection
                    ## this connection just takes 
                    ## the old connections place in the queue
                    db_conn = pymysql.connect(
                        host        =self.settings["CONNECTION"]["HOST"],
                        port        =self.settings["CONNECTION"]["PORT"],
                        user        =self.settings["CONNECTION"]["USER"],
                        passwd      =self.settings["CONNECTION"]["PASSWORD"],
                        db          =self.settings["CONNECTION"]["DATABASE"],
                    );
                    logging.debug("created db_conn to replace bad")
                    pass
                except Exception:
                    logging.debug("error creating db_conn to replace bad")
                    raise
        return db_conn

    def return_db_conn(self, db_conn):
        """Puts a connection back in the pool.
        Relies on gevent (db_pool is a Queue).
        Does nothing if we have no db_pool.
        """
        if not self.db_pool == None:
            self.db_pool.put_nowait(db_conn)

    def init_db_pool(self, pool_size=10):
        """create our MySQL connections pool.
        Queue used for pool relies on gevent.
        """
        logging.debug("init_db_pool")
        try:
            # Only create it if it doesn't exist
            if self.db_conn == None and self.db_pool == None:
                logging.debug("need to create new db_pool")
                self.db_pool = gevent.queue.Queue() 
                for i in range(pool_size): 
                    self.db_pool.put_nowait(create_db_conn(self.settings)) 
            else:
                logging.debug("NOT creating db_pool")
        except Exception:
            self.set_db_pool(None)
            logging.debug("error creating db_pool")
            raise

    def init_db_conn(self):
        """create our MySQL connections"""
        logging.debug("init_db_conn")
        try:
            # Only create it if it doesn't exist
            if self.db_conn == None and self.db_pool == None:
                logging.debug("need to create new db_conn")
                self.db_conn = create_db_conn(self.settings)
            else:
                logging.debug("NOT creating db_conn")
        except Exception:
            self.set_db_conn(None)
            logging.debug("error creating db_conn")
            raise


    """ some MySQL helper functions to keep Queryset code cleaner
    """

    def item_exists(self, table, id):
        """check if an item exists using an integer id"""
        logging.debug("item_exists")
        db_conn = None
        cursor = None
        try:
            db_conn = self.get_db_conn()
            sql = "SELECT count(*) FROM `%s`  WHERE id = %%s" % (table)
            sql = self.escape_sql(sql, [id], self.db_conn)
            cursor = db_conn.cursor()
            cursor.execute (sql)
            row = cursor.fetchone ()
        except:
            raise
        finally:
            cursor.close()
            self.return_db_conn(db_conn)
        if row == None or row[0] == 0:
            return False
        return true

    def escape_sql(self, sql, args, db_conn):
        # escape sql here so we can debug the real query string
        if args is not None:
            if isinstance(args, tuple) or isinstance(args, list):
                escaped_args = tuple(db_conn.escape(arg) for arg in args)
            elif isinstance(args, dict):
                escaped_args = dict((key, db_conn.escape(val)) for (key, val) in args.items())
            else:
                #If it's not a dictionary let's try escaping it anyways.
                #Worst case it will throw a Value error
                escaped_args = db_conn.escape(args)        
            try:
                sql = sql % escaped_args
            except:
                pass
        logging.debug(sql)
        return sql

    def execute(self, sql, args = None, is_insert = False, is_insert_update = False):
        """performs an insert, update or delete"""
        logging.debug("execute")
        
        affected_rows = 0
        inserted_id = None
        
        db_conn = self.get_db_conn()
        cursor = db_conn.cursor()
        sql = self.escape_sql(sql, args, db_conn)
        try:
            affected_rows = cursor.execute (sql)
            if (is_insert or is_insert_update) and affected_rows == 1:
                inserted_id = cursor.lastrowid    
            db_conn.commit()
        except:
            db_conn.rollback()
            raise
        finally:
            cursor.close()
            self.return_db_conn(db_conn)
        if is_insert or is_insert_update:
            return (affected_rows, inserted_id)
        return affected_rows

    def query(self, sql, args=None, format=FORMAT_DICT, fetch_one=False):
        """performs a query.
           Defaults to returning a dict object, since that is what a DICT models and JSON need
        """
        logging.debug("query")
        db_conn = self.get_db_conn()
        sql = self.escape_sql(sql, args, db_conn)
        cursor = None
        rows = None
        if format == self.FORMAT_TUPLE:
            logging.debug("tuple")
            cursor = db_conn.cursor()
        else:
            logging.debug("dict")
            cursor = db_conn.cursor(cursors.DictCursor)
        try:
            cursor.execute(sql)
            if fetch_one == True:
                logging.debug("fetch_one")
                rows = cursor.fetchone()
            else:
                logging.debug("fetch_all")
                rows = cursor.fetchall()
            logging.debug("db_conn:%s" % db_conn)
        except:
            logging.debug("ERROR!!!!!!!!!")
            raise
        finally:
            cursor.close()
            db_conn.commit()
            self.return_db_conn(db_conn)
        return rows

    def fetch(self, sql, args=None, format=FORMAT_DICT):
        """gets just one item, the first returned"""
        logging.debug("fetch")
        row = self.query(sql, args, format, True)
        if row == None or len(row) == 0:
            return None
        return  row

    def get_fields_list(self):
        """Creates a MySQL safe list of field names"""
        if self.fields == None:
            raise Exception("attribute fields not set in queryset!")
        # create a function to wrap and join our field names
        def wrap_and_join(field):
            return '`' + str(field) + '`'
        # map each item in the list and return us
        return ','.join(map(wrap_and_join, self.fields))

    def get_table_name(self):
        if self.table_name == None:
            raise Exception("attribute table_name not set in queryset!")


class MySqlApiQueryset(MySqlQueryset, AbstractQueryset):
    """implement all our auto API functions mixin for MySql backed Queryset objects"""

    def __init__(self, settings, db_pool):
        super(MySqlApiQueryset, self).__init__(settings, db_pool)
        self.fields_muteable = None     # A list of field names that can be updated


    def get_values_list(self, shield):
        """Creates a MySQL safe list of field values
            1. The format string for the sql
            2. A list of the values themselves
        """
        if self.fields == None:
            raise Exception("attribute fields not set in queryset!")
        # create a function to wrap and join our field names
        def wrap_and_join(field):
            # if we are not a number field, wrap us in quotes
            f = getattr(shield, field)
            if f != None and f._jsonschema_type() == 'string':
                return "'%s'" % field
            return str(field)
        def get_value(field):
            return getattr(shield, field)
        # map each item in the list and return us
        return (','.join(map(wrap_and_join, self.fields)), map(get_value, self.fields))

    def get_insert_fields_equal_values_list(self, shield):
        """Creates a MySQL safe list of field values
        returns a tuple containing:
            1. The format string for the sql
            2. A list of the values themselves
        """
        if self.fields == None:
            raise Exception("attribute fields not set in queryset!")
        return self._get_fields_equal_values_list(shield, self.fields)

    def get_update_fields_equal_values_list(self, shield):
        """Creates a MySQL safe list of field values
            returns a tuple containing:
            1. The format string for the sql
            2. A list of the values themselves
        """
        if self.fields_muteable == None:
            raise Exception("attribute fields_muteable not set in queryset!")
        return self._get_fields_equal_values_list(shield, self.fields_muteable)

    def _dictshield_to_mysql_formatter(self, shield, field):
        """
        This method returns a string formatter to be used in constructing the
        sql string to eacape.
        At the moment it is limited to simple types.
        Complex or container types are not supported.
        (SEE: https://github.com/j2labs/dictshield/tree/master/docs)
        """
        string_formatter = '%s' # this is used for everything (% escapes %)
        dictfield = shield._fields[field]
        #logging.debug("dictfield for %s" % field)
        #logging.debug(dictfield)
        if isinstance(dictfield, dictshield.fields.StringField):
            # A unicode string
            return string_formatter
        elif isinstance(dictfield, dictshield.fields.URLField):
            # A valid URL
            return string_formatter
        elif isinstance(dictfield, dictshield.fields.EmailField):
            # A valid email address
            return string_formatter
        elif isinstance(dictfield, dictshield.fields.UUIDField):
            # A valid UUID value, optionally auto-populates empty values with new UUIDs
            return string_formatter
        elif isinstance(dictfield, MongoFields.ObjectIdField):
            # Wraps a MongoDB "BSON" ObjectId
            return string_formatter
        elif isinstance(dictfield, dictshield.fields.NumberField):
            # Any number (the parent of all the other numeric fields)
            return string_formatter
        elif isinstance(dictfield, dictshield.fields.IntField):
            # An integer
            return string_formatter
        elif isinstance(dictfield, dictshield.fields.LongField):
            # A long
            return string_formatter
        elif isinstance(dictfield, dictshield.fields.FloatField):
            # A float
            return string_formatter
        elif isinstance(dictfield, dictshield.fields.DecimalField):
            # A fixed-point decimal number
            return string_formatter
        elif isinstance(dictfield, dictshield.fields.MD5Field):
            # An MD5 hash
            return string_formatter
        elif isinstance(dictfield, dictshield.fields.SHA1Field):
            # An SHA1 hash
            return string_formatter
        elif isinstance(dictfield, dictshield.fields.BooleanField):
            # A boolean
            return string_formatter
        elif isinstance(dictfield, dictshield.fields.DateTimeField):
            # A datetime
            return string_formatter
        elif isinstance(dictfield, dictshield.fields.GeoPointField):
            # A geo-value of the form x, y (latitude, longitude)
            raise Exception("GeoPointField not Supported")
        elif isinstance(dictfield, CompoundFields.ListField):
            # Wraps a standard field, so multiple instances of the field can be used
            raise Exception("ListField not Supported")
        elif isinstance(dictfield, CompoundFields.SortedListField):
            # A ListField which sorts the list before saving, so list is always sorted
            raise Exception("SortedListField not Supported")
        elif isinstance(dictfield, CompoundFields.DictField):
            # Wraps a standard Python dictionary
            raise Exception("DictField not Supported")
        elif isinstance(dictfield, CompoundFields.MultiValueDictField):
            # Wraps Django's implementation of a MultiValueDict.
            raise Exception("MultiValueDictField not Supported")
        elif isinstance(dictfield, CompoundFields.EmbeddedDocumentField):
            # A whole other entity
            raise Exception("EmbeddedDocumentField not Supported")

    def _dictshield_to_mysql_value(self, shield, field):
        """
        This method returns a value that can be stored in the database
        At the moment it is limited to simple types.
        Complex or container types are not supported.
        """
        field_value = ''
        dictfield = shield._fields[field]
        if isinstance(dictfield, dictshield.fields.StringField):
            # A unicode string
            field_value = getattr(shield, field)
        elif isinstance(dictfield, dictshield.fields.URLField):
            # A valid URL
            field_value = getattr(shield, field)
        elif isinstance(dictfield, dictshield.fields.EmailField):
            # A valid email address
            field_value = getattr(shield, field)
        elif isinstance(dictfield, dictshield.fields.UUIDField):
            # A valid UUID value, optionally auto-populates empty values with new UUIDs
            field_value = getattr(shield, field)
        elif isinstance(dictfield, MongoFields.ObjectIdField):
            # Wraps a MongoDB "BSON" ObjectId
            field_value = getattr(shield, field)
        elif isinstance(dictfield, dictshield.fields.NumberField):
            # Any number (the parent of all the other numeric fields)
            field_value = getattr(shield, field)
        elif isinstance(dictfield, dictshield.fields.IntField):
            # An integer
            field_value = getattr(shield, field)
        elif isinstance(dictfield, dictshield.fields.LongField):
            # A long
            field_value = getattr(shield, field)
        elif isinstance(dictfield, dictshield.fields.FloatField):
            # A float
            field_value = getattr(shield, field)
        elif isinstance(dictfield, dictshield.fields.DecimalField):
            # A fixed-point decimal number
            field_value = getattr(shield, field)
        elif isinstance(dictfield, dictshield.fields.MD5Field):
            # An MD5 hash
            field_value = getattr(shield, field)
        elif isinstance(dictfield, dictshield.fields.SHA1Field):
            # An SHA1 hash
            field_value = getattr(shield, field)
        elif isinstance(dictfield, dictshield.fields.BooleanField):
            # A boolean
            field_value = getattr(shield, field)
        elif isinstance(dictfield, dictshield.fields.DateTimeField):
            # A datetime
            field_value = getattr(shield, field)
        elif isinstance(dictfield, dictshield.fields.GeoPointField):
            # A geo-value of the form x, y (latitude, longitude)
            raise Exception("GeoPointField not Supported")
        elif isinstance(dictfield, CompoundFields.ListField):
            # Wraps a standard field, so multiple instances of the field can be used
            raise Exception("ListField not Supported")
        elif isinstance(dictfield, CompoundFields.SortedListField):
            # A ListField which sorts the list before saving, so list is always sorted
            raise Exception("SortedListField not Supported")
        elif isinstance(dictfield, CompoundFields.DictField):
            # Wraps a standard Python dictionary
            raise Exception("DictField not Supported")
        elif isinstance(dictfield, CompoundFields.MultiValueDictField):
            # Wraps Django's implementation of a MultiValueDict.
            raise Exception("MultiValueDictField not Supported")
        elif isinstance(dictfield, CompoundFields.EmbeddedDocumentField):
            # A whole other entity
            raise Exception("EmbeddedDocumentField not Supported")
        return field_value


    def _get_fields_equal_values_list(self, shield, fields):
        """Creates a MySQL safe list of field values
            returns a tuple containing:
            1. The format string for the sql
            2. A list of the values themselves
        """
        # create a function to wrap and join our field names
        def wrap_and_join(field):
            return "%s=%s" % (field, self._dictshield_to_mysql_formatter(shield, field))
        def get_value(field):
            return self._dictshield_to_mysql_value(shield, field)
        # map each item in the list and return us
        formatter = ','.join(map(wrap_and_join, fields))
        values = map(get_value, fields)
        return (formatter, values)

    ###
    ### Start functions nedded for auto API
    ###
    ## Create Functions

    def create_one(self, shield, **kw):
        logging.debug("create_one")
        # be pesimistic, alway assume failure
        status = self.MSG_FAILED 
        # check for our optional table_name argument
        table_name = self.table_name if not 'table_name' in kw else kw['table_name']
        insert_info = self.get_insert_fields_equal_values_list(shield)
        update_info = self.get_update_fields_equal_values_list(shield)
        sql = """
            INSERT INTO `%s` 
            set %s 
            ON DUPLICATE KEY UPDATE
            %s
            """ % (table_name, insert_info[0], update_info[0])
        (affected_rows, inserted_id) = self.execute(sql, 
            insert_info[1] + update_info[1], is_insert_update = True )
        if affected_rows == 1:
            status = self.MSG_CREATED
        elif affected_rows == 2:
            status = self.MSG_UPDATED
        logging.debug("create_one (status, affected_rows): (%s, %s)" % (status, affected_rows))
        if inserted_id != None:
            shield.id = inserted_id
            logging.debug("inserted_id: %s)" % (inserted_id))
        return (status, shield)

    def create_many(self, shields, **kw):
        table_name = self.table_name if not 'table_name' in kw else kw['table_name']
        statuses = [self.create_one(shield, table_name=table_name) for shield in shields]
        return statuses

    ## Read Functions

    def read_all(self, **kw):
        table_name = self.table_name if not 'table_name' in kw else kw['table_name']
        return [(self.MSG_OK, datum) for datum in self.query("SELECT %s FROM `%s`" % (self.get_fields_list(), table_name))]

    def read_one(self, iid, **kw):
        logging.debug("read_one")
        table_name = self.table_name if not 'table_name' in kw else kw['table_name']
         # be pesimistic, alway assume failure
        status = self.MSG_FAILED 
        iid = int(iid)  # id is always an int in MySQL
        sql = "SELECT %s FROM `%s` WHERE ID = %%s" % (self.get_fields_list(), table_name)
        #logging.debug("sql: %s" % sql)
        item = self.fetch(sql, [iid])
        if item != None:
            return (self.MSG_OK, item)
        return (status, iid)

    def read_many(self, ids, **kw):
        try:
            table_name = self.table_name if not 'table_name' in kw else kw['table_name']
            return [self.read_one(iid, table_name) for iid in ids]
        except KeyError:
            raise FourOhFourException

    ## Update Functions

    def update_one(self, shield, **kw):
        logging.debug("update_one")
        table_name = self.table_name if not 'table_name' in kw else kw['table_name']
        # be pesimistic, alway assume failure
        status = self.MSG_FAILED 
        sql = """
            UPDATE `%s` 
            SET %S
            WHERE id = %%s
        """ % (table_name, self.get_fields_equal_values_list(shield))
        if self.execute(sql, tuple(shield.id)):
            status = self.MSG_CREATED
        return (status, shield)

    def update_many(self, shields, **kw):
        table_name = self.table_name if not 'table_name' in kw else kw['table_name']
        statuses = [self.update_one(shield, table_name=table_name) for shield in shields]
        return statuses

    ## Destroy Functions

    def destroy_one(self, iid, **kw):
        logging.debug("destroy_one")
        table_name = self.table_name if not 'table_name' in kw else kw['table_name']
        # be pesimistic, alway assume failure
        status = self.MSG_FAILED 
        iid = int(iid)  # id is always an int in MySQL
        try:
            sql = """
                DELETE FROM `%s`
                WHERE id = %%s LIMIT 1
            """ % (table_name)
            if self.execute(sql, [iid]):
                return (self.MSG_UPDATED, iid)
        except KeyError:
            raise FourOhFourException
        return (status, iid)

    def destroy_many(self, ids, **kw):
        table_name = self.table_name if not 'table_name' in kw else kw['table_name']
        statuses = [self.destroy_one(iid, table_name=table_name) for iid in ids]
        return statuses

    ###
    ### end functions nedded for auto API
    ###
