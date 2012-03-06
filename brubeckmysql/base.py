# Copyright 2012 Brooklyn Code Incorporated. See LICENSE.md for usage
# the license can also be found at http://brooklyncode.com/opensource/LICENSE.md

import pymysql
import logging

##
## This is a method so we can create our DB connection and pass it to the application, the proper Brubeck way.
## If we don't store our connection in the application, we will create one for each MySqlQueryset created
##
## Here are the example settings for creating the connection
##
""" 
mysql = {
    "CONNECTION": {
        "HOST": "127.0.0.1",               ## MySQL Host
        "PORT": 3306,                      ## MySQL Post
        "USER": "[YOUR USERNAME HERE]",    ## MySQL User
        "PASSWORD": "[YOUR PASSWORD HERE]", ## MySQL Password
        "DATABASE": "[YOUR DATABASE HERE]", ## Database Name
        "COLLATION": 'utf8',               ## Database Collation
    }
"""

def create_db_conn(settings):
    """create our MySQL connection"""
    logging.debug("create_db_conn")
    db_conn = None
    try:
        # Only create it if it doesn't exist
        # logging.debug("creating db_conn")
        ## create our mySql connection
        db_conn = pymysql.connect(
            host    =settings["CONNECTION"]["HOST"],
            port    =settings["CONNECTION"]["PORT"],
            user    =settings["CONNECTION"]["USER"],
            passwd  =settings["CONNECTION"]["PASSWORD"],
            db      =settings["CONNECTION"]["DATABASE"]
        );
        logging.debug("created db_conn")

    except Exception:
        logging.debug("error creating db_conn")
        raise
    return db_conn
