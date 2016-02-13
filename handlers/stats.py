import flask

import chdb
import config
from common import *

import os
import json
import re

def load_is_crawler(stats_db):
    crawler_user_agents = json.load(
        file(os.path.join(
            os.path.dirname(__file__),
            'crawler-user-agents', 'crawler-user-agents.json')))
    with stats_db as cursor:
        cursor.execute('DROP FUNCTION IF EXISTS IS_CRAWLER')
        cursor.execute('''
            CREATE FUNCTION IS_CRAWLER (user_agent VARCHAR(1024))
            RETURNS BOOLEAN DETERMINISTIC RETURN (''' +
            ' OR '.join(
                'user_agent REGEXP "%s"' % obj['pattern']
                for obj in crawler_user_agents) + ')')

def stats():
    days = flask.request.args.get('days', 14)
    lang_codes = sorted(config.lang_code_to_config.keys())

    def rows_to_data_table(header, rows):
        data_rows = []
        for date, count, lang_code in rows:
            if lang_code not in config.lang_code_to_config:
                continue
            # find the row corresponding to this date
            for r in data_rows:
                if r[0] == date:
                    dr = r
                    break
            else:
                # add a row if it doesn't exist yet
                dr = [date] + [0] * len(lang_codes)
                data_rows.append(dr)

            dc = lang_codes.index(lang_code) + 1
            dr[dc] = count
        return [[header] + lang_codes] + data_rows

    graphs = [] # title, data table as array, type
    stats_db = get_stats_db()
    load_is_crawler(stats_db)
    stats_cursor = stats_db.cursor()
    lang_cursors = {lc: get_db(lc).cursor() for lc in lang_codes}

    stats_cursor.execute('''
        SELECT DATE_FORMAT(ts, GET_FORMAT(DATE, 'ISO')) AS dt,
        COUNT(*), lang_code FROM requests
        WHERE snippet_id IS NOT NULL AND status_code = 200
        AND DATEDIFF(NOW(), ts) <= %s AND IS_CRAWLER(user_agent) = 0
        GROUP BY dt, lang_code ORDER BY dt, lang_code''', (days,))
    graphs.append((
        'Number of snippets served in the past %s days' % days,
        json.dumps(rows_to_data_table('Date', list(stats_cursor))), 'line'))

    for lc in lang_codes:
        data_rows = []
        stats_cursor.execute('''
            SELECT category_id, COUNT(*), user_agent FROM requests
            WHERE snippet_id IS NOT NULL AND category_id IS NOT NULL AND
            category_id != "all" AND status_code = 200
            AND DATEDIFF(NOW(), ts) <= %s AND lang_code = %s
            AND IS_CRAWLER(user_agent) = 0
            GROUP BY category_id ORDER BY COUNT(*) DESC LIMIT 30
        ''', (days, lc))
        for category_id, count, user_agent in stats_cursor:
            c = lang_cursors[lc]
            c.execute('''
                SELECT title FROM categories WHERE id = %s''', (category_id,))
            title = list(c)[0][0] if c.rowcount else None
            data_rows.append((title, count))
        graphs.append((
            '30 most popular categories in the past %s days, %s' % (days, lc),
            json.dumps([['Category', 'Count']] + data_rows), 'table'))

    return flask.render_template('stats.html', graphs = graphs)

