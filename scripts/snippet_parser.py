#!/usr/bin/env python

from __future__ import unicode_literals

from utils import *

import wikitools
import mwparserfromhell
import subprocess

import re
import sys
import urlparse
import hashlib

WIKIPEDIA_BASE_URL = 'https://en.wikipedia.org'
WIKIPEDIA_WIKI_URL = WIKIPEDIA_BASE_URL + '/wiki/'
WIKIPEDIA_API_URL = WIKIPEDIA_BASE_URL + '/w/api.php'

MARKER = '7b94863f3091b449e6ab04d44cb372a0' # unlikely to be in any article

TEST_WIKITEXT_CACHE_FILENAME = '.test-wikitext.cache'

def is_citation_needed(t):
    return t.name.matches('Citation needed') or t.name.matches('cn')

# Monkey-patch mwparserfromhell so it strips some templates and tags the way
# we want.
def template_strip(self, normalize, collapse):
    if not is_citation_needed(self):
        return self
    return ''
mwparserfromhell.nodes.Template.__strip__ = template_strip

def tag_strip(self, normalize, collapse):
    if self.tag == 'ref':
        return None
    return self._original_strip(normalize, collapse)
mwparserfromhell.nodes.Tag._original_strip = mwparserfromhell.nodes.Tag.__strip__
mwparserfromhell.nodes.Tag.__strip__ = tag_strip

mwparserfromhell.nodes.Heading.__strip__ = mwparserfromhell.nodes.Node.__strip__

def wikilink_strip(self, normalize, collapse):
    if self.title.startswith('File:'):
        return ''
    return self._original_strip(normalize, collapse)
mwparserfromhell.nodes.Wikilink._original_strip = \
    mwparserfromhell.nodes.Wikilink.__strip__
mwparserfromhell.nodes.Wikilink.__strip__ = wikilink_strip

def extract_snippets(wikitext, minlen = 140, maxlen = 420, is_lead = False):
    snippets = [] # [section, [snippets]]
    strip_regexp = re.compile('\s+' + MARKER) # strip spaces before MARKER

    sections = mwparserfromhell.parse(wikitext).get_sections(
        include_lead = True, include_headings = True, flat = True)
    assert ''.join(unicode(s) for s in sections) == d(wikitext)

    for i, section in enumerate(sections):
        assert i == 0 or \
            isinstance(section.get(0), mwparserfromhell.nodes.heading.Heading)
        sectitle = unicode(section.get(0).title.strip()) if i != 0 else ''
        secsnippets = []
        snippets.append([sectitle, secsnippets])

        for paragraph in section.split('\n\n'):
            wikicode = mwparserfromhell.parse(paragraph)

            for t in wikicode.filter_templates():
                if is_citation_needed(t):
                    stripped_len = len(wikicode.strip_code())
                    if stripped_len > maxlen or stripped_len < minlen:
                        # TL;DR or too short
                        continue

                    # add the marker so we know where the Citation-needed
                    # template was
                    wikicode.insert_before(t, MARKER)

            cmd = [
                'python', 'smc/mw/tool.py', '-p', '-T',
                '/Users/Guiherme/code/ch-venv/citationhunt/scripts/templates/'
            ]
            env = {'PYTHONPATH': '.'}
            cwd = '/Users/Guiherme/src/smc.mw/'

            proc = subprocess.Popen(cmd, stdin = subprocess.PIPE, stdout = subprocess.PIPE, cwd = cwd, env = env)
            stdout, stderr = proc.communicate(e(wikicode.strip_code()))
            if proc.returncode == 0 and stdout:
                snippet = d(stdout)
            else:
                snippet = wikicode.strip_code()

            snippet = re.sub(strip_regexp, MARKER, snippet)
            if MARKER in snippet: # MARKER may have been inside wiki markup
                secsnippets.append(snippet)
    return snippets

if __name__ == '__main__':
    import pprint

    title = sys.argv[1]
    wikitext = None
    try:
        with open(TEST_WIKITEXT_CACHE_FILENAME, 'r') as cache:
            if cache.readline()[:-1] == title:
                wikitext = cache.read()
    except:
        pass
    finally:
        if wikitext is None:
            wikipedia = wikitools.wiki.Wiki(WIKIPEDIA_API_URL)
            page = wikitools.Page(wikipedia, title)
            wikitext = page.getWikiText()

    with open(TEST_WIKITEXT_CACHE_FILENAME, 'w') as cache:
        print >>cache, title
        cache.write(wikitext)

    pprint.pprint(extract_snippets(wikitext))
