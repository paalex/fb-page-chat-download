#!/usr/bin/env python
# encoding: utf-8

"""
How to use:
 * Go to https://developers.facebook.com/tools/explorer and click 'Get User Access Token'
 * Then select 'manage_pages' and 'read_page_mailboxes'
 * Switch to a page that you want to scrape
 * Get the page_id and the token and pass as parameters to this script
"""

"python3.7 run.py <Page_id> output.txt <Page_token>"

"Socky 480550808645602"

"Genius page 1963659870547080"
"python3.7 run.py 1963659870547080 output.txt "

"TVP page 152157788181214"
"python3.7 run.py 152157788181214 output.txt "

import os
import csv
import json
import requests
import argparse
import sys
import re
import datetime
import unidecode
import time

class FBScraper:
    def __init__(self, page, output, token, since=None, until=None):
        self.token = token
        self.output = output
        self.since = since
        self.until = until
        conversations_limit = '10'
        self.uri = self.build_url('{}/conversations?fields=participants,link&limit=' + conversations_limit, page)
        self.archived_uri = self.build_url('{}/conversations?fields=participants,link&limit=' + conversations_limit + '&tags=action:archived', page)

    def build_url(self, endpoint, *params):
        return "https://graph.facebook.com/v2.6/" + endpoint.format(*params) + '&access_token={}'.format(self.token)

    def scrape_thread(self, url, lst):
        if self.since:
            matches = re.findall('&until=(\d+)', url)
            if matches and int(matches[0]) <= self.since:
                return lst

        messages = requests.get(url).json()
        for m in messages['data']:
            time = datetime.datetime.strptime(m['created_time'], '%Y-%m-%dT%H:%M:%S+0000').replace(tzinfo=datetime.timezone.utc).timestamp()

            if self.since and time < self.since:
                continue
            if self.until and time > self.until:
                continue
            lst.append({
                'time': m['created_time'].replace('+0000', '').replace('T', ' '),
                'message': m['message'],
                'attachments': m.get('attachments', {}).get('data', [{}])[0].get('image_data', {}).get('url', ''),
                'shares': m.get('shares', {}).get('data', [{}])[0].get('name', ''),
                'from_id': m['from']['id']
            })
        if messages['data']:
            print(' +', len(messages['data']))
        next = messages.get('paging', {}).get('next', '')
        if next:
            self.scrape_thread(next, lst)
        return lst


    def scrape_thread_list(self, threads, count):
        for t in threads['data']:
            extra_params = (('&since=' + str(self.since)) if self.since else '') + (('&until=' + str(self.until)) if self.until else '')
            messages_page_limit = '100'
            url = self.build_url('{}/messages?fields=from,created_time,message,shares,attachments&limit=' + messages_page_limit + extra_params, t['id'])
            print("GET", unidecode.unidecode(t['participants']['data'][0]['name']), t['id'])
            thread = self.scrape_thread(url, [])
            if thread:
                self.writer.writerow({
                    # 'page_id': t['participants']['data'][1]['id'],
                    # 'page_name': t['participants']['data'][1]['name'],
                    # 'user_id': t['participants']['data'][0]['id'],
                    # 'user_name': t['participants']['data'][0]['name'],
                    'url': t['link'],
                })
            id_map = {p['id']: p['name'] for p in t['participants']['data']}
            for message in reversed(thread):
                message['from'] = id_map[message['from_id']]
                self.writer.writerow(message)
            time.sleep(1.1)

        next = threads.get('paging', {}).get('next', '')
        print("next page ",count, next)
        if next and count > 1:
            time.sleep(1)
            self.scrape_thread_list(requests.get(next).json(), count - 1)


    def run(self):
        output = open(self.output, 'w', newline="\n", encoding="utf-8")
        threads = requests.get(self.uri).json()
        if 'error' in threads:
            print(threads)
            return

        threads_archived = requests.get(self.archived_uri).json()

        fieldnames = ['from_id', 'from', 'time', 'message', 'attachments', 'shares', 'url']
        self.writer = csv.DictWriter(output, dialect='excel', fieldnames=fieldnames, extrasaction='ignore', quoting=csv.QUOTE_NONNUMERIC)
        self.writer.writerow(dict((n, n) for n in fieldnames))
        conversation_pages_limit = 400 # total messages amount =< conversation_pages_limit * conversations_limit
        self.scrape_thread_list(threads, conversation_pages_limit)
        # raw_url = '' // continue from a middle point
        # self.scrape_thread_list(requests.get(raw_url).json(), conversation_pages_limit)
        self.scrape_thread_list(threads_archived, conversation_pages_limit)
        output.close()

def main():
    """
        Main method
    """
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('page', metavar='page_id', type=int, nargs=1, help='Facebook Page ID')
    parser.add_argument('output', metavar='output_file', type=str, nargs=1, help='CSV Output File')
    parser.add_argument('token', metavar='access_token', type=str, nargs=1, help='Access Token')
    parser.add_argument('--since', metavar='since_epoch', type=int, nargs='?', help='Filter messages from after this time')
    parser.add_argument('--until', metavar='until_epoch', type=int, nargs='?', help='Filter messages from before this time')
    args = parser.parse_args()
    FBScraper(args.page[0], args.output[0], args.token[0], args.since, args.until).run()

if __name__ == "__main__":
    main()
