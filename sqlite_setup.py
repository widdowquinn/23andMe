#!/usr/bin/env python
#
# sqlite_setup.py
#
# Creates a new SQLite3 database to contain 23andMe-associated data

###
# IMPORTS

from optparse import OptionParser
from process_exceptions import last_exception

import logging
import logging.handlers
import os
import re
import sys
import sqlite3


###
# FUNCTIONS

# Parse cmd-line
def parse_cmdline(args):
    """Parse command-line arguments. Note that the database filename is
    a positional argument.
    """
    usage = "usage: %prog [options] <database>"
    parser = OptionParser(usage)
    parser.add_option("-v", "--verbose", dest="verbose",
                      action="store_true", default=False,
                      help="Give verbose output")
    return parser.parse_args()


# Connect to database
def get_db_connection(filename):
    """Make a connection to an SQLite database."""
    try:
        conn = sqlite3.connect(filename)
    except:
        logger.error("Failed to connect to SQLite db %s (exiting)" % filename)
        logger.error(last_exception())
        sys.exit(1)
    return conn


# Create database views
def create_db_views(conn):
    """Create views on the database."""
    sql_snpcount = """ DROP VIEW IF EXISTS snp_counts;
                       CREATE VIEW snp_counts
                         AS SELECT snp_id, COUNT(*) as count
                              FROM genotypes GROUP BY snp_id;
                   """
    # Create each view in turn
    for tname, sql in [("snpcount", sql_snpcount)]:
        with conn:
            cur = conn.cursor()
            try:
                logger.info("Creating view %s" % tname)
                cur.executescript(sql)
            except:
                logger.error("Could not create view % (exiting)" % tname)
                logger.error(last_exception())
                sys.exit(1)


# Create database tables
def create_db_tables(conn):
    """Create the tables for the SQLite3 database."""
    # SQL for each table creation
    sql_snp = """ DROP TABLE IF EXISTS snp_location;
                  CREATE TABLE snp_location (snp_id TEXT,
                                             hg_version TEXT,
                                             chromosome TEXT,
                                             position INTEGER,
                                             PRIMARY KEY(snp_id, hg_version));
              """
    sql_gtype = """ DROP TABLE IF EXISTS genotypes;
                    CREATE TABLE genotypes
                        (snp_id TEXT NOT NULL,
                         genotype TEXT NOT NULL,
                         PRIMARY KEY(snp_id, genotype),
                         FOREIGN KEY(snp_id) REFERENCES snp(snp_id)
                           ON DELETE CASCADE);
                """
    sql_person = """ DROP TABLE IF EXISTS person;
                     CREATE TABLE person
                         (person_id INTEGER PRIMARY KEY AUTOINCREMENT,
                          name TEXT)
                 """
    sql_person_gtype = """ DROP TABLE IF EXISTS person_gtype;
                           CREATE TABLE person_gtype
                             (person_id INTEGER NOT NULL,
                              snp_id INTEGER NOT NULL,
                              genotype TEXT NOT NULL,
                              PRIMARY KEY(person_id, snp_id, genotype),
                              FOREIGN KEY(person_id)
                                REFERENCES person(person_id)
                                ON DELETE CASCADE,
                              FOREIGN KEY(snp_id, genotype)
                                REFERENCES snp(snp_id, genotype)
                                ON DELETE CASCADE);
                        """
    # Create each table in turn
    for tname, sql in [("snp_location", sql_snp),
                       ("genotype", sql_gtype),
                       ("person", sql_person),
                       ("person_gtype", sql_person_gtype)]:
        with conn:
            cur = conn.cursor()
            try:
                logger.info("Creating table %s" % tname)
                cur.executescript(sql)
            except:
                logger.error("Could not create table % (exiting)" % tname)
                logger.error(last_exception())
                sys.exit(1)

###
# SCRIPT

if __name__ == '__main__':
    # Parse command-line
    options, args = parse_cmdline(sys.argv)

    # We set up logging, and modify loglevel according to whether we need
    # verbosity or not
    logger = logging.getLogger('sqlite_setup.py')
    logger.setLevel(logging.DEBUG)
    err_handler = logging.StreamHandler(sys.stderr)
    err_formatter = logging.Formatter('%(levelname)s: %(message)s')
    err_handler.setFormatter(err_formatter)
    if options.verbose:
        err_handler.setLevel(logging.INFO)
    else:
        err_handler.setLevel(logging.WARNING)
    logger.addHandler(err_handler)

    # Report arguments, if verbose
    logger.info(options)
    logger.info(args)

    # Throw an error if we don't have positional arguments
    if len(args) != 1:
        logger.error("Not enough arguments: script requires database name " +
                     "(exiting)")
        sys.exit(1)

    # Get conection to database
    conn = get_db_connection(args[0])

    # Create database tables
    create_db_tables(conn)

    # Create database views
    create_db_views(conn)
