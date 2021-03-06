#!/usr/bin/env python

'''
Parser for the pages+articles XML dump for CitationHunt.

Given a file with one pageid per line, this script will find unsourced
snippets in the pages in the pageid file. It will store the pages containing
valid snippets in the `articles` database table, and the snippets in the
`snippets` table.

Usage:
    parse_pages_articles.py <pages-articles-xml.bz2> <pageid-file>
'''

from __future__ import unicode_literals

import os
import sys
sys.path.append('../')

import chdb
import config
import snippet_parser
import workerpool
from utils import *

import docopt
import mwparserfromhell

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

import signal
import bz2file
import pickle
import itertools
import urllib

cfg = config.get_localized_config()
WIKIPEDIA_BASE_URL = 'https://' + cfg.wikipedia_domain
WIKIPEDIA_WIKI_URL = WIKIPEDIA_BASE_URL + '/wiki/'

NAMESPACE_ARTICLE = '0'

log = Logger()

def section_name_to_anchor(section):
    # See Sanitizer::escapeId
    # https://doc.wikimedia.org/mediawiki-core/master/php/html/classSanitizer.html#ae091dfff62f13c9c1e0d2e503b0cab49
    section = section.replace(' ', '_')
    # urllib.quote interacts really weirdly with unicode in Python2:
    # https://bugs.python.org/issue23885
    section = urllib.quote(e(section), safe = e(''))
    section = section.replace('%3A', ':')
    section = section.replace('%', '.')
    return section

class RowParser(workerpool.Worker):
    def setup(self):
        self.parser = snippet_parser.get_localized_snippet_parser()

    def work(self, task):
        kind, info = task
        assert kind == 'article'

        pageid, title, wikitext = info
        url = WIKIPEDIA_WIKI_URL + title.replace(' ', '_')

        snippets_rows = []
        snippets = self.parser.extract_snippets(
            wikitext, cfg.snippet_min_size, cfg.snippet_max_size)
        for sec, snips in snippets:
            sec = section_name_to_anchor(sec)
            for sni in snips:
                id = mkid(title + sni)
                row = (id, sni, sec, pageid)
                snippets_rows.append(row)

        if snippets_rows:
            article_row = (pageid, url, title)
            return (kind, {'article': article_row, 'snippets': snippets_rows})
        return (kind, {})

    def done(self):
        pass

# FIXME originally we needed only a single process writing to the database,
# because sqlite3 sucks at multiprocessing. We can probably change that with
# MySQL.
class DatabaseWriter(workerpool.Receiver):
    def __init__(self):
        self.chdb = None

    def setup(self):
        self.chdb = chdb.reset_scratch_db()

    def receive(self, task):
        kind, rows = task
        assert kind == 'article'
        self.write_article_rows(rows)

    def write_article_rows(self, rows):
        if not rows:
            return

        def insert(cursor):
            cursor.execute('''
                INSERT INTO articles VALUES(%s, %s, %s)''', rows['article'])
            cursor.executemany('''
                INSERT IGNORE INTO snippets VALUES(%s, %s, %s, %s)''',
                rows['snippets'])
        self.chdb.execute_with_retry(insert)

    def done(self):
        self.chdb.close()

def handle_article(wp, element, pageids, stats):
    # elements are not pickelable, so we can't pass them to workers. extract
    # all the relevant information here and offload only the wikicode
    # parsing.

    id = d(element.find('id').text)
    if id not in pageids:
        return
    pageids.remove(id)

    if element.find('redirect') is not None:
        stats['redirect'].append(id)
        return

    title = d(element.find('title').text)
    text = element.find('revision/text').text
    if text is None:
        stats['empty'].append(id)
        return
    text = d(text)

    wp.post(('article', (id, title, text)))
    return

def parse_xml_dump(pages_articles_xml_bz2, pageids):
    count = 0
    stats = {'redirect': [], 'empty': [], 'pageids': None}
    iterparser = ET.iterparse(bz2file.BZ2File(pages_articles_xml_bz2))

    parser = RowParser()
    writer = DatabaseWriter()
    wp = workerpool.WorkerPool(parser, writer)
    for _, element in iterparser:
        element.tag = element.tag[element.tag.rfind('}')+1:]
        if element.tag == 'page':
            ns = element.find('ns').text
            if ns == NAMESPACE_ARTICLE:
                handle_article(wp, element, pageids, stats)
            count += 1
            if count % 10 == 0:
                log.progress('processed about %d pages' % count)
            element.clear()
        if canceled:
            log.info('canceled, killing process pool...')
            wp.cancel()
            return
    wp.done()
    stats['pageids'] = pageids

    if len(pageids) > 0:
        log.info('%d pageids were not found' % len(stats['pageids']))
    log.info('%d pages were redirects' % len(stats['redirect']))
    log.info('%d pages were empty' % len(stats['empty']))
    with open('stats.pkl', 'wb') as statsf:
        pickle.dump(stats, statsf)

if __name__ == '__main__':
    canceled = False
    def set_canceled_flag(sig, stack):
        global canceled
        canceled = True
        signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.signal(signal.SIGINT, set_canceled_flag)

    arguments = docopt.docopt(__doc__)
    xml_dump_filename = arguments['<pages-articles-xml.bz2>']
    pageids_file = arguments['<pageid-file>']
    with open(pageids_file) as pf:
        pageids = set(itertools.imap(str.strip, pf))
    parse_xml_dump(xml_dump_filename, pageids)
    log.info('all done.')
    if canceled:
        os.kill(os.getpid(), signal.SIGINT)
