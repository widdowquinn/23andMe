#!/usr/bin/env python
#
# snpedia.py
#
# Helper functions for pulling data from SNPedia (http://www.snpedia.com/)
# SNPedia: a wiki supporting personal genome annotation, interpretation and 
#          analysis 
# Michael Cariaso; Greg Lennon
# Nucleic Acids Research 2011; doi: 10.1093/nar/gkr798
#
# The SNPedia API is located at http://bots.snpedia.com/api.php
#
# This module based on simplemediawiki.py
#
# Example queries
# ===============
# 1) Get the page corresponding to a particular SNP: rs737865
# http://www.snpedia.com/api.php?format=json&action=query&titles=rs737865

###
# IMPORTS

import cookielib
import gzip
import json
import re
import urllib
import urllib2
import xml.etree.ElementTree as ET

from StringIO import StringIO


SNPEDIA_ROOT = "http://www.snpedia.com/api.php?"
USER_AGENT = "snpedia.py/0.1"

###
# CLASSES

class SNPedia(object):
    """ Create a new object to access SNPedia.

        endpoint is the MediaWiki API endpoint. This should be the default,
        but let's let people be flexible.
    """
    def __init__(self, endpoint=SNPEDIA_ROOT, useragent=USER_AGENT):
        self._endpoint = endpoint
        self._cj = cookielib.CookieJar()
        self._opener = urllib2.build_opener(
                                  urllib2.HTTPCookieProcessor(self._cj))
        self._opener.addheaders = [('User-Agent', useragent)]

    def _fetch(self, url, params):
        """ HTTP request handler with gzip and cookie support
        """
        params['format'] = 'json'
        request = urllib2.Request(url, urllib.urlencode(params))
        request.add_header('Accept-encoding', 'gzip')
        response = self._opener.open(request)
        if response.headers.get('Content-Encoding') == 'gzip':
            compressed_data = StringIO(response.read())
            data = gzip.GzipFile(fileobj=compressed_data).read()
        else:
            data = response.read()
        return data

    def call(self, params):
        """ Make an API call to SNPedia using the params dictionary of 
            query string arguments. For example, to get the page ID for 
            SNP rs737865, call
            SNPedia.call({'action': 'query',
                          'titles': 'rs737865'})
        """
        response = self._fetch(self._endpoint, params)
        #print response
        return json.loads(response)

    def query(self, params):
        """ Make an API query call to SNPedia
        """
        params['action'] = 'query'
        return self.call(params)

    def get_page_by_snp_id(self, snp_id):
        """ Returns the SNPedia page IDs for a passed SNP, identified by its
            SNP ID
        """
        params = {'titles': snp_id}
        response = self.query(params)
        page = response['query']['pages'].keys()
        assert len(page) == 1, "Expected a single page for this SNP"
        return page[0]

    def get_text_by_snp_id(self, snp_id):
        """ Returns the collated text (as Mediawiki markup) from SNPedia 
            pages corresponding to the passed SNP id
        """
        params = {'titles': snp_id,
                  'export': None}
        response = self.query(params)
        # Process response to extract the <text /> element
        xml = response['query']['export']['*']
        tree = ET.parse(StringIO(xml))
        root = tree.getroot()
        # Not sure what's to be done about the URI in braces - maybe a regex
        # replace for \{.*\}
        # There should only be one text element for the page
        text = list(root.iter('{http://www.mediawiki.org/xml/export-0.7/}text'))
        assert len(text) == 1, "Expected one text element for this page"
        return text[0].text

###
# FUNCTIONS

# Clean up Mediawiki text
def clean_mediawiki_markup(text):
    """ Cleans up mediawiki markup. Removes square brackets, which indicate 
        links (external and internal). Also removes the double-braced markup
    """
    newtext = text.replace('[','').replace(']','')
    regex = '\}\}\s*\{\{|'
    newtext = re.subn(regex, '', newtext)[0]
    regex = '\{\{.*?\}\}'
    newtext = re.subn(regex, '', newtext, flags=re.DOTALL)[0]
    return newtext
    
# Extract PMIDs from Mediawiki text
def get_pmids_from_mediawiki_markup(text):
    """ Identifies PMID accessions from SNPedia MediaWiki markup, returning
        a list of accessions.
        The SNPedia markup is either {{PMID|<accession}} or
        {{PMID Auto|PMID=<accession>|
    """
    regex1 = '(?<=\{\{PMID\|)[0-9]*(?=\}\})'
    accessions = [m for m in re.findall(regex1, text)]
    regex2 = '(?<=PMID=)[0-9]*'
    accessions.extend([m for m in re.findall(regex2, text)])
    return accessions
    
# Get population diversity data from MediaWiki page
def get_population_diversity_from_mediawiki_markup(text):
    """ Extracts population diversity information, returning a tuple of 
        dictionaries: (genotypes, diversity), where -
        genotypes: {'geno1': '(A;B)', ...}
        diversity: {'CEU': (x, y, z), ...}
        where A, B are the bases of the genotype, and x, y, z are the 
        percentage occurrence of genotypes 1, 2 and 3 in the population 
        indicated by the key.
    """
    # Isolate population diversity text
    regex = "(?<=\{\{\spopulation\sdiversity\|\s).*?(?=HapMap)"
    data = re.search(regex, text.replace('\n',''))
    if data is None:
        print "Did not find population diversity data for this SNP"
        return
    match = [e.strip() for e in data.group().split('|')]
    gdata = [e for e in match if e.startswith('geno')]
    ddata = [e for e in match if not e.startswith('geno') and len(e)]
    genotypes = {}
    diversity = {}
    for gt in gdata:
        g, b = gt.split('=')
        genotypes[g] = b
    print ddata
    for idx in range(0, len(ddata), len(genotypes)+1):
        diversity[ddata[idx]] = tuple(ddata[idx+1:idx+1+len(genotypes)])
    return genotypes, diversity

# Get PharmGKB data from MediaWiki page
def get_pharmgkb_from_mediawiki_markup(text):
    """ Extracts PharmGKB data from SNPedia page, and returns the information
        as a list of strings
    """
    # Isolate PharmGKB data
    regex = "(?<=\{\{PharmGKB\|).*?(?=\}\})"
    data = re.findall(regex, text.replace('\n',''))
    if data is None:
        print "Did not find PharmGKB data for this SNP"
        return
    return data

# Get OMIM data from MediaWiki page
def get_omim_from_mediawiki_markup(text):
    """ Extracts OMIM data from SNPedia page, and returns the information
        as a list of strings
    """
    # Isolate OMIM data
    regex = "(?<=\{\{omim\|).*?(?=\}\})"
    data = re.findall(regex, text.replace('\n',''))
    if data is None:
        print "Did not find OMIM data for this SNP"
        return
    return data

# Get rsnum data from MediaWiki page
def get_rsnum_from_mediawiki_markup(text):
    """ Extracts OMIM data from SNPedia page, and returns the information
        as a string
    """
    # Isolate rsnum data
    regex = "(?<=\{\{Rsnum\|).*?(?=\}\})"
    data = re.search(regex, text.replace('\n',''))
    if data is None:
        print "Did not find OMIM data for this SNP"
        return
    return data.group()


###
# SCRIPT

if __name__ == '__main__':
    # Instantiate a SNPedia connection object, and call for the page of our
    # example SNP
    snpedia = SNPedia()
    # Should return a page ID
    page = snpedia.get_page_by_snp_id('rs737865')
    print page
    # Shouldn't return a page ID
    page = snpedia.get_page_by_snp_id('foo-bar')
    print page
    # Get page text for a SNP
    text = snpedia.get_text_by_snp_id('rs737865')
    print clean_mediawiki_markup(text)
    print get_pmids_from_mediawiki_markup(text)
    print get_population_diversity_from_mediawiki_markup(text)
    print get_pharmgkb_from_mediawiki_markup(text)
    print get_omim_from_mediawiki_markup(text)
    print get_rsnum_from_mediawiki_markup(text)
