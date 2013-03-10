#!/usr/bin/env python
#
# full_to_sqlite.py
#
# Script to load 23andMe full data for an individual into an SQLite3 database
# prepared using the sqlite_setup.py script

###
# IMPORTS 

from optparse import OptionParser
from process_exceptions import last_exception

import csv
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
    """ Parse command-line arguments. Note that the database filename is
        a positional argument
    """
    usage = "usage: %prog [options] <name> <snpfile> <database>"
    parser = OptionParser(usage)
    parser.add_option("-n", "--newdb", dest="newdb",
                      action="store_true", default=False,
                      help="Set to true if this is the first entry in " +\
                          "the database")
    parser.add_option("-v", "--verbose", dest="verbose",
                      action="store_true", default=False,
                      help="Give verbose output")
    return parser.parse_args()

# Connect to database
def get_db_connection(filename):
    """ Make a connection to an SQLite database
    """
    try:
        logger.info("Connecting to database: %s" % filename)
        conn = sqlite3.connect(filename)
        logger.info("Connection made")
    except:
        logger.error("Failed to connect to SQLite db %s (exiting)" % filename)
        logger.error(last_exception())
        sys.exit(1)
    return conn

# Parse data from file and populate database
def populate_db(name, snpfile, conn, newdb=False):
    """ Using the database with the connection in conn, add data for the named
        person from the passed snpfile
    """
    # Get filehandle
    try:
        fh = open(snpfile, 'rU')
    except:
        logger.error("Could not open SNP file %s" % snpfile)
        logger.error(last_exception())
        sys.exit(1)
    # Load data into database
    logger.info("Processing %s" % snpfile)
    with fh as snpfh:
        with conn:
            cur = conn.cursor()
            # Populate the person table. At the mom
            sql = "INSERT INTO person(name) VALUES (?)" 
            cur.execute(sql, (name, ))
            person_id = cur.lastrowid
            # Parse the SNP file
            snpreader = csv.reader(snpfh, delimiter='\t')
            try:
                for row in snpreader:
                    if not row[0].startswith('#'):   # ignore comments
                        rsid, chrm, pos, gt = tuple(row)
                        pos = int(pos)
                        # Add snp, if necessary:
                        if newdb:
                            sql = "INSERT INTO snp(snp_id, chromosome, " +\
                                "position) VALUES (?, ?, ?)"
                            cur.execute(sql, (rsid, chrm, pos))
                        # Insert genotype
                        try:
                            sql = "INSERT INTO genotypes(snp_id, genotype) " +\
                                "VALUES (?, ?)"
                            cur.execute(sql, (rsid, gt))
                        except sqlite3.IntegrityError:
                            # This will throw an error if the genotype exists
                            # in the db
                            logger.warning("Genotype %s already " % gt +\
                                            "present for SNP %s" % rsid)
                            pass
                        # Link individual to genotype
                        try:
                            sql = "INSERT INTO person_gtype(person_id, " +\
                                     "snp_id, genotype) VALUES (?, ?, ?)"
                            cur.execute(sql, (person_id, rsid, gt))
                        except:
                            logger.error("Problem populating database at " +\
                                             "row %s (exiting)" % row)
                            sys.exit(1)
            except:
                logger.error("Problem parsing SNP file %s" % snpfile)
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
    err_formatter = \
                  logging.Formatter('%(levelname)s: %(message)s')
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
    if len(args) != 3:
        logger.error("Script requires three arguments, %d given " % len(args) +\
                         "(exiting)")
        sys.exit(1)

    # Make database connection
    conn = get_db_connection(args[2])

    # Populate database from file
    populate_db(args[0], args[1], conn, newdb=options.newdb)
