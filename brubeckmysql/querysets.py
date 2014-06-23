#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright 2012 Brooklyn Code Incorporated. See LICENSE.md for usage
# the license can also be found at http://brooklyncode.com/opensource/LICENSE.md
import json
import logging
import os
import imp
import time
import uuid
import datetime

import pymysql
from pymysql.connections import Connection
from pymysql import cursors
from brubeck.queryset import AbstractQueryset
from schematics.types import compound as CompoundFields
from schematics.types import mongo as MongoFields
from base import create_db_conn_pool
from base import create_db_conn
import schematics
from gevent.queue import Queue

import re, htmlentitydefs

##
# Removes HTML or XML character references and entities from a text string.
#
# @param text The HTML (or XML) source text.
# @return The plain text, as a Unicode string, if necessary.


def unescape(text):
    """Thanks http://effbot.org/zone/re-sub.htm#unescape-html"""
    def fixup(m):
        text = m.group(0)
        if text[:2] == "&#":
            # character reference
            try:
                if text[:3] == "&#x":
                    return unichr(int(text[3:-1], 16))
                else:
                    return unichr(int(text[2:-1]))
            except ValueError:
                pass
        else:
            # named entity
            try:
                text = unichr(htmlentitydefs.name2codepoint[text[1:-1]])
            except KeyError:
                pass
        return text # leave as is
    return re.sub("&#?\w+;", fixup, text)

###
### All of our data interaction with any data store happens in a Queryset object
###

class MySqlQueryset(object):
    """base class mixin for MySql backed Queryset objects"""

    # Used to set the format of our returned object
    # FORMAT_DICT is a reasonable default since Brubeck
    # uses that to create both Schematic objects and JSON responses

    FORMAT_TUPLE = 0
    FORMAT_DICT  = 1
    FORMAT_DICTSHIELD  = 2

    MSG_NOCHANGES  = 'NO CHANGES'

    def __init__(self, settings, db_conn, table_tag, auto_commit = None, **kw):
        """load our settings and do minimal config"""
        logging.debug("MySqlQueryset __init__ for %s with auto_commit=%s initializing" %
                      (table_tag, auto_commit))
        logging.debug("MySqlQueryset __init__ db_conn=%s (%s)" %
                      (db_conn, db_conn.__class__ if not db_conn is None else 'None'))
        self.settings = settings
        if auto_commit is None:
            auto_commit = True
        self.auto_commit = auto_commit
        if isinstance(db_conn, Queue):
            self.db_pool = db_conn
            self.db_conn = None
        else:
            self.db_conn = db_conn
            self.db_pool = None

        if self.db_pool is None and self.db_conn is None:
            logging.debug("None db_conn passed to queryset __init__t")

        # We will need to set these in the entity specific implmentation
        # Once Schematic has more meta data, this may not be necessary anymore
        self.table_name = None          # the name of the database table
        self.fields = None              # A list of field names
        if table_tag is None:
            # We will need to set these in the entity specific implementation
            # Once Schematic has more meta data, this may not be necessary anymore
            self.table_name = None
            self.fields = None
            self.fields_muteable = None
        else:
            self.table_name = self.settings["TABLES"][table_tag]["TABLE_NAME"]
            self.fields = self.settings["TABLES"][table_tag]["FIELDS"]
            self.fields_muteable = self.settings["TABLES"][table_tag]["FIELDS_MUTEABLE"]

        logging.debug("MySqlQueryset __init__ table_name=%s, fields=%s, fields_muteable=%s" %
                      (self.table_name, self.fields, self.fields_muteable))

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
        if self.db_conn is None and self.db_pool is None:
            logging.debug('MySqlQueryset get_db_conn db_conn and db_pool is None')
            self.init_db_conn()
        # get a connection
        if not self.db_conn is None:
            # not using pooling, or
            # we already got our connection from pool earlier
            logging.debug('MySqlQueryset get_db_conn returning existing db_conn')
            db_conn = self.db_conn
        elif not self.db_pool is None:
            # Will block until one becomes available
            logging.debug('MySqlQueryset get_db_conn getting db_conn from pool')
            db_conn = self.db_pool.get()

        if not db_conn is None:
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
                        host = self.settings["CONNECTION"]["HOST"],
                        port = self.settings["CONNECTION"]["PORT"],
                        user = self.settings["CONNECTION"]["USER"],
                        passwd = self.settings["CONNECTION"]["PASSWORD"],
                        db = self.settings["CONNECTION"]["DATABASE"],
                    );
                    logging.debug("created db_conn to replace bad")
                    pass
                except Exception:
                    logging.debug("error creating db_conn to replace bad")
                    raise
        return db_conn

    def commit(self):
        """commits any uncommited transactions"""
        try:
            self.get_db_conn().commit()
        except Exception as e:
            pass

    def return_db_conn(self, db_conn):
        """Puts a connection back in the pool.
        Relies on gevent (db_pool is a Queue).
        Does nothing if we have no db_pool.
        """
        if not self.db_pool is None:
            self.db_pool.put_nowait(db_conn)

    def init_db_pool(self, pool_size=10):
        """create our MySQL connections pool.
        Queue used for pool relies on gevent.
        """
        logging.debug("init_db_pool")
        try:
            # Only create it if it doesn't exist
            if self.db_conn is None and self.db_pool is None:
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
            if self.db_conn is None and self.db_pool is None:
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
            sql = self.escape_sql(sql, [id], db_conn)
            cursor = db_conn.cursor()
            cursor.execute (sql)
            row = cursor.fetchone ()
        except:
            raise
        finally:
            if cursor is not None:
                cursor.close()
            self.return_db_conn(db_conn)
        if row is None or row[0] == 0:
            return False
        return True

    def escape_sql(self, sql, args, db_conn):
        # escape sql here so we can debug the real query string
        #logging.debug("escape_sql sql: %s" % sql)
        #logging.debug("escape_sql args: %s" % args)
        def coerce_value(val):
            #logging.debug("Coercing value: %s" % val)
            # convert datetime args to epoch
            if isinstance(val, datetime.datetime):
                val = "%s.%s" % (long(time.mktime(val.timetuple())), val.microsecond)
            return val
        if args is not None:
            if isinstance(args, tuple) or isinstance(args, list):
                escaped_args = tuple(db_conn.escape(coerce_value(arg)) for arg in args)
            elif isinstance(args, dict):
                escaped_args = dict((key, db_conn.escape(coerce_value(val))) for (key, val) in args.items())
            else:
                #If it's not a dictionary let's try escaping it anyways.
                #Worst case it will throw a Value error
                escaped_args = db_conn.escape(args)
            try:
                sql = sql % escaped_args
            except:
                raise
                pass
        else:
            # escape just in case for format fields
            # that should double escape %
            sql = sql % ()
        logging.debug("Escaped SQL to execute: %s" % sql)
        return sql

    def execute(self, sql, args = None, is_insert = False,
                is_insert_update = False, commit = None):
        """performs an insert, update or delete"""
        if commit is None:
            commit = self.auto_commit
        #logging.debug("execute")
        affected_rows = 0
        inserted_id = None
        db_conn = self.get_db_conn()
        cursor = db_conn.cursor()
        #logging.debug("execute args: %s" % args)
        sql = self.escape_sql(sql, args, db_conn)
        #logging.debug("execute: %s" % sql)
        try:
            affected_rows = cursor.execute (sql)
            if (is_insert or is_insert_update) and affected_rows == 1:
                inserted_id = cursor.lastrowid
            if commit == True:
                db_conn.commit()
        except:
            if commit == True:
                db_conn.rollback()
            raise
        finally:
            if cursor is not None:
                cursor.close()
            self.return_db_conn(db_conn)
        if is_insert or is_insert_update:
            return (affected_rows, inserted_id)
        return affected_rows

    def query(self, sql, args=None, format=FORMAT_DICT, fetch_one=False, include_field_names=False):
        """performs a query.
           Defaults to returning a dict object, since that is what a DICT models and JSON need
        """
        #logging.debug("query")
        db_conn = self.get_db_conn()
        sql = self.escape_sql(sql, args, db_conn)
        cursor = None
        rows = None
        field_names = None
        if format == self.FORMAT_TUPLE:
            cursor = db_conn.cursor()
        else:
            cursor = db_conn.cursor(cursors.DictCursor)
        try:
            cursor.execute(sql)
            if fetch_one == True:
                logging.debug("fetch_one")
                rows = cursor.fetchone()
            else:
                logging.debug("fetch_all")
                rows = cursor.fetchall()
            logging.debug("query db_conn:%s" % db_conn)
            if include_field_names:
                field_names = cursor._fields
        except:
            logging.debug("ERROR!!!!!!!!!")
            raise
        finally:
            if cursor is not None:
                cursor.close()
            self.return_db_conn(db_conn)
        if field_names:
            return (rows, field_names)
        else:
            return rows

    def fetch(self, sql, args=None, format=FORMAT_DICT):
        """gets just one item, the first returned"""
        #logging.debug("fetch")
        row = self.query(sql, args, format, True)
        if row is None or len(row) == 0:
            return None
        return  row

    def get_select_fields_list(self, alias = None):
        return self.get_fields_list(alias, 'select')

    def get_fields_list(self, alias = None, action = None):
        """Creates a MySQL safe list of field names"""
        if self.fields is None:
            raise Exception("attribute fields not set in queryset!")

        if alias is not None:
            alias = '`%s`.' % alias
        else:
            alias = ''

        # create a function to wrap and join our field names
        def wrap_and_join(field):
            #logging.debug('field: %s' % field)
            if isinstance(field, dict):
                field_name = field['name']
                if not action is None and action == 'select':
                    field_alias = field_name
                    field_format='%s'
                    if 'alias' in field:
                        field_alias = field['alias']
                    if 'read_format' in field:
                        field_format = field['read_format']
                    field_name = '%s`%s`' % (alias,str(field_name))
                    field = '%s as `%s`' % ((field_format % field_name), field_alias)
                else:
                    field = field_name
                    field = '%s`%s`' % (alias,str(field))
            else:
                field = '%s`%s`' % (alias,str(field))

            #logging.debug('return field: %s' % field)
            return field

        # map each item in the list and return us
        fields_list = ','.join(map(wrap_and_join, self.fields))
        return fields_list

    def get_table_name(self):
        if self.table_name is None:
            raise Exception("attribute table_name not set in queryset!")


class MySqlApiQueryset(MySqlQueryset, AbstractQueryset):
    """implement all our auto API functions mixin for MySql backed Queryset objects"""

    def __init__(self, settings, db_pool, table_tag = None, auto_commit = None):
        """load our settings and do minimal config"""
        logging.debug("MySqlAPIQueryset for %s with auto_commit=%s initializing" %
                      (table_tag, auto_commit))
        logging.debug("MySqlAPIQueryset db_pool=%s (%s)" %
                      (db_pool, db_pool.__class__ if not db_pool is None else 'None'))
        if auto_commit is None:
            auto_commit = True
        self.auto_commit = auto_commit
        super(MySqlApiQueryset, self).__init__(settings, db_pool,
                               table_tag, auto_commit)
        self.fields_muteable = None     # A list of field names that can be updated

        if table_tag is not None:
            self.table_name = self.settings["TABLES"][table_tag]["TABLE_NAME"]
            self.fields = self.settings["TABLES"][table_tag]["FIELDS"]
            self.fields_muteable = self.settings["TABLES"][table_tag]["FIELDS_MUTEABLE"]

    def dictListToSchematicList(self, dict_items):
        items = []
        for dict_item in dict_items:
            items.append(self.DictToSchematic(dict_item))
        return items

    def resultsTupleToSchematicList(self, result_tuples):
        items = []
        for result_tuple in result_tuples:
            if result_tuple[0] == self.MSG_OK:
                items.append(self.DictToSchematic(result_tuple[1]))
        return items

    def DictToSchematic(self, dict_value):
        """Take a Queryset result dict and return a Schematic"""
        raise NotImplemented("DictToSchematic(self, dict_value) needed in MySqlApiQueryset.")

    def get_values_list(self, shield):
        """Creates a MySQL safe list of field values
            1. The format string for the sql
            2. A list of the values themselves
        """
        if self.fields is None:
            raise Exception("attribute fields not set in queryset!")
        # create a function to wrap and join our field names
        def wrap_and_join(field):
            # if we are not a number field, wrap us in quotes
            f = getattr(shield, field)
            if not f is None and f._jsonschema_type() == 'string':
                return u"'%s'" % field
            return str(field)
        def get_value(field):
            return getattr(shield, field).encode('utf8')
        # map each item in the list and return us
        return (','.join(map(wrap_and_join, self.fields)), map(get_value, self.fields))

    def get_insert_fields_equal_values_list(self, shield):
        """Creates a MySQL safe list of field values
        returns a tuple containing:
            1. The format string for the sql
            2. A list of the values themselves
        """
        if self.fields is None:
            raise Exception("attribute fields not set in queryset!")
        return self._get_fields_equal_values_list(shield, self.fields)

    def get_update_fields_equal_values_list(self, shield):
        """Creates a MySQL safe list of field values
            returns a tuple containing:
            1. The format string for the sql
            2. A list of the values themselves
        """
        if self.fields_muteable is None:
            raise Exception("attribute fields_muteable not set in queryset!")
        return self._get_fields_equal_values_list(shield, self.fields_muteable)

    def _schematic_to_mysql_formatter(self, shield, field):
        """
        This method returns a string formatter to be used in constructing the
        sql string to escape.
        At the moment it is limited to simple types.
        Complex or container types are not supported.
        (SEE: https://github.com/j2labs/schematic/tree/master/docs)
        """
        string_formatter = u'%s' # this is used for everything (% escapes %)
        #logging.debug("shield._fields %s" % shield._fields)
        #logging.debug("field %s" % field)
        if isinstance(field, dict):
            field = field['name']

        if field not in shield._fields and field == 'id':
            return string_formatter;

        myfield = shield._fields[field]
        #logging.debug("dictfield for %s" % field)
        #logging.debug(dictfield)
        if isinstance(myfield, schematics.types.StringType):
            # A unicode string
            return string_formatter
        elif isinstance(myfield, schematics.types.URLType):
            # A valid URL
            return string_formatter
        elif isinstance(myfield, schematics.types.EmailType):
            # A valid email address
            return string_formatter
        elif isinstance(myfield, schematics.types.UUIDType):
            # A valid UUID value, optionally auto-populates empty values with new UUIDs
            return string_formatter
        elif isinstance(myfield, MongoFields.ObjectIdType):
            # Wraps a MongoDB "BSON" ObjectId
            return string_formatter
        elif isinstance(myfield, schematics.types.NumberType):
            # Any number (the parent of all the other numeric fields)
            return string_formatter
        elif isinstance(myfield, schematics.types.IntType):
            # An integer
            return string_formatter
        elif isinstance(myfield, schematics.types.LongType):
            # A long
            return string_formatter
        elif isinstance(myfield, schematics.types.FloatType):
            # A float
            return string_formatter
        elif isinstance(myfield, schematics.types.DecimalType):
            # A fixed-point decimal number
            return string_formatter
        elif isinstance(myfield, schematics.types.MD5Type):
            # An MD5 hash
            return string_formatter
        elif isinstance(myfield, schematics.types.SHA1Type):
            # An SHA1 hash
            return string_formatter
        elif isinstance(myfield, schematics.types.BooleanType):
            # A boolean
            return string_formatter
        elif isinstance(myfield, schematics.types.DateTimeType):
            # A datetime
            return string_formatter
        elif isinstance(myfield, schematics.types.GeoPointType):
            # A geo-value of the form x, y (latitude, longitude)
            raise Exception("GeoPointField not Supported")
        elif isinstance(myfield, CompoundFields.ListType):
            # Wraps a standard field, so multiple instances of the field can be used
            raise Exception("ListField not Supported")
        elif isinstance(myfield, CompoundFields.SortedListType):
            # A ListField which sorts the list before saving, so list is always sorted
            raise Exception("SortedListField not Supported")
        elif isinstance(myfield, CompoundFields.DictType):
            # Wraps a standard Python dictionary
            raise Exception("DictField not Supported")
        elif isinstance(myfield, CompoundFields.MultiValueDictType):
            # Wraps Django's implementation of a MultiValueDict.
            raise Exception("MultiValueDictField not Supported")
        elif isinstance(myfield, CompoundFields.ModelType):
            # A whole other entity
            raise Exception("ModelType not Supported")

    def _schematic_to_mysql_value(self, shield, field):
        """
        This method returns a value that can be stored in the database
        At the moment it is limited to simple types.
        Complex or container types are not supported.
        """
        field_value = u''
        #logging.debug("shield._fields %s" % shield._fields)
        #logging.debug("field %s" % field)
        if isinstance(field, dict):
            field = field['name']

        if field not in shield._fields:
            return None;

        myfield = shield._fields[field]

        if isinstance(myfield, schematics.types.StringType):
            # A unicode string
            field_value = getattr(shield, field)
        elif isinstance(myfield, schematics.types.URLType):
            # A valid URL
            field_value = getattr(shield, field)
        elif isinstance(myfield, schematics.types.EmailType):
            # A valid email address
            field_value = getattr(shield, field)
        elif isinstance(myfield, schematics.types.UUIDType):
            # A valid UUID value, optionally auto-populates empty values with new UUIDs
            field_value = getattr(shield, field)
        elif isinstance(myfield, MongoFields.ObjectIdType):
            # Wraps a MongoDB "BSON" ObjectId
            field_value = getattr(shield, field)
        elif isinstance(myfield, schematics.types.NumberType):
            # Any number (the parent of all the other numeric fields)
            field_value = getattr(shield, field)
        elif isinstance(myfield, schematics.types.IntType):
            # An integer
            field_value = getattr(shield, field)
        elif isinstance(myfield, schematics.types.LongType):
            # A long
            field_value = getattr(shield, field)
        elif isinstance(myfield, schematics.types.FloatType):
            # A float
            field_value = getattr(shield, field)
        elif isinstance(myfield, schematics.types.DecimalType):
            # A fixed-point decimal number
            field_value = getattr(shield, field)
        elif isinstance(myfield, schematics.types.MD5Type):
            # An MD5 hash
            field_value = getattr(shield, field)
        elif isinstance(myfield, schematics.types.SHA1Type):
            # An SHA1 hash
            field_value = getattr(shield, field)
        elif isinstance(myfield, schematics.types.BooleanType):
            # A boolean
            field_value = getattr(shield, field)
        elif isinstance(myfield, schematics.types.DateTimeType):
            # A datetime
            field_value = getattr(shield, field)
        elif isinstance(myfield, schematics.types.GeoPointType):
            # A geo-value of the form x, y (latitude, longitude)
            raise Exception("GeoPointField not Supported")
        elif isinstance(myfield, CompoundFields.ListType):
            # Wraps a standard field, so multiple instances of the field can be used
            raise Exception("ListField not Supported")
        elif isinstance(myfield, CompoundFields.SortedListType):
            # A ListField which sorts the list before saving, so list is always sorted
            raise Exception("SortedListField not Supported")
        elif isinstance(myfield, CompoundFields.DictType):
            # Wraps a standard Python dictionary
            raise Exception("DictField not Supported")
        elif isinstance(myfield, CompoundFields.MultiValueDictType):
            # Wraps Django's implementation of a MultiValueDict.
            raise Exception("MultiValueDictField not Supported")
        return field_value


    def _get_fields_equal_values_list(self, shield, fields):
        """Creates a MySQL safe list of field values
            returns a tuple containing:
            1. The format string for the sql
            2. A list of the values themselves
        """
        # create a function to wrap and join our field names
        def wrap_and_join(field):
            logging.debug('field: %s' % field)
            if isinstance(field, dict):
                field_name = field['name']
                field_format='%s'
                # we may need to format our objects value for mysql
                if 'write_format' in field:
                    field_write_format = field['write_format']
                    field_format = self._schematic_to_mysql_formatter(shield, field)
                field_format_value = field_write_format % field_format
                return u"%s=%s" % (field_name, field_format_value )

            return u"%s=%s" % (field, self._schematic_to_mysql_formatter(shield, field))

        def get_value(field):
            if isinstance(field, dict):
                field = field['name']
            val = self._schematic_to_mysql_value(shield, field)
            #if isinstance(val, unicode):
            #    val = val.encode('utf8')
            return val
        # map each item in the list and return us
        formatter = u','.join(map(wrap_and_join, fields))
        values = map(get_value, fields)
        return (formatter, values)

    ###
    ### Start functions nedded for auto API
    ###
    ## Create Functions

    def create_one(self, shield, commit = None, **kw):
        logging.debug("MySqlApiQueryset create_one")
        if commit is None:
            commit = self.auto_commit
        # be pesimistic, alway assume failure
        status = self.MSG_FAILED
        # check for our optional table_name argument
        table_name = self.table_name if not 'table_name' in kw else kw['table_name']
        insert_info = self.get_insert_fields_equal_values_list(shield)
        update_info = self.get_update_fields_equal_values_list(shield)
        if update_info[0] == '':
            sql = u"""
                INSERT INTO `%s`
                set %s
                """ % (table_name, insert_info[0])
        else:
            sql = u"""
                INSERT INTO `%s`
                set %s
                ON DUPLICATE KEY UPDATE
                %s
                """ % (table_name, insert_info[0], update_info[0])
        logging.debug("MySqlApiQueryset create_one sql: %s" % sql)
        (affected_rows, inserted_id) = self.execute(sql,
            insert_info[1] + update_info[1],
            is_insert = True, is_insert_update = True,
            commit = commit )
        if affected_rows == 1:
            status = self.MSG_CREATED
        elif affected_rows == 2:
            status = self.MSG_UPDATED
        else:
            # we may have executed a query with no changes needed, so check if the item exists
            if self.item_exists(table_name, shield.id):
                status = self.MSG_NOCHANGES

        logging.debug("MySqlApiQueryset create_one (status, affected_rows): (%s, %s)" % (status, affected_rows))
        if not inserted_id is None:
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
        return [(self.MSG_OK, datum) for datum in self.query(u"SELECT %s FROM `%s`" % (self.get_select_fields_list(), table_name))]

    def read_one(self, iid, **kw):
        logging.debug("MySqlApiQueryset read_one")
        table_name = self.table_name if not 'table_name' in kw else kw['table_name']
         # be pesimistic, alway assume failure
        status = self.MSG_FAILED
        iid = int(iid)  # id is always an int in MySQL
        sql = u"SELECT %s FROM `%s` WHERE ID = %%s" % (self.get_select_fields_list(), table_name)
        #logging.debug("sql: %s" % sql)
        item = self.fetch(sql, [iid])
        if not item is None:
            return (self.MSG_OK, item)
        return (status, iid)

    def read_many(self, ids, **kw):
        try:
            table_name = self.table_name if not 'table_name' in kw else kw['table_name']
            return [self.read_one(iid, table_name) for iid in ids]
        except KeyError:
            raise FourOhFourException

    ## Update Functions

    def update_one(self, shield, commit = None, **kw):
        logging.debug("MySqlApiQueryset update_one")
        if commit is None:
            commit = self.auto_commit
        table_name = self.table_name if not 'table_name' in kw else kw['table_name']
        # be pesimistic, alway assume failure
        status = self.MSG_FAILED
        sql = u"""
            UPDATE `%s`
            SET %S
            WHERE id = %%s
        """ % (table_name, self.get_fields_equal_values_list(shield))
        if self.execute(sql, tuple(shield.id),
                        is_insert = False, is_insert_update = False,
                        commit = commit):
            status = self.MSG_CREATED
        return (status, shield)

    def update_many(self, shields, **kw):
        table_name = self.table_name if not 'table_name' in kw else kw['table_name']
        statuses = [self.update_one(shield, table_name=table_name) for shield in shields]
        return statuses

    ## Destroy Functions

    def destroy_one(self, iid, commit = None, **kw):
        logging.debug("MySqlApiQueryset destroy_one")
        if commit is None:
            commit = self.auto_commit
        table_name = self.table_name if not 'table_name' in kw else kw['table_name']
        # be pesimistic, alway assume failure
        status = self.MSG_FAILED
        iid = int(iid)  # id is always an int in MySQL
        try:
            sql = u"""
                DELETE FROM `%s`
                WHERE id = %%s LIMIT 1
            """ % (table_name)
            if self.execute(sql, [iid],
                            is_insert = False, is_insert_update = False,
                            commit = commit):
                return (self.MSG_UPDATED, iid)
        except KeyError:
            raise FourOhFourException
        return (status, iid)

    def destroy_many(self, ids, commit = None, **kw):
        table_name = self.table_name if not 'table_name' in kw else kw['table_name']
        statuses = [self.destroy_one(iid, table_name=table_name, commit=commit) for iid in ids]
        return statuses

    ###
    ### end functions nedded for auto API
    ###
