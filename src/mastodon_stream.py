import mastodon
from mastodon import Mastodon
from bs4 import BeautifulSoup
import argparse
import datetime
from threading import Timer
import os
from aux2.mastodon.token import *
from aux2.kafka.kafka_m_producer import kafka_m_producer

# globals
base_url = ''
topic_name, producer = '' , ''



# Listener for Mastodon events
class Listener(mastodon.StreamListener):

    def on_update(self, status):
        m_text = BeautifulSoup(status.content, 'html.parser').text
        num_tags = len(status.tags)
        num_chars = len(m_text)
        num_words = len(m_text.split())
        m_lang = status.language
        if m_lang is None:
            m_lang = 'unknown'
        m_user = status.account.username

        app = ''
        # attribute only available on local
        if hasattr(status, 'application'):
            try:
                app = status.application.get('name')
            except:
                app = ''

        now_dt=datetime.datetime.now()
        
        value_dict = { 
            'm_id': status.id,
            'created_at': int(now_dt.timestamp()),
            'created_at_str': now_dt.strftime('%Y %m %d %H:%M:%S'),
            'app': app,
            'url': status.url,
            'base_url': base_url,  
            'language': m_lang, 
            'favourites': status.favourites_count, 
            'username': m_user, 
            'bot': status.account.bot, 
            'tags': num_tags, 
            'characters': num_chars, 
            'words': num_words, 
            'mastodon_text': m_text
        }
        
        producer.produce(topic = topic_name, value = value_dict)
        producer.flush()


def main():
    global base_url
    global quiet
    global watchdog
    global topic_name, producer


    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '--baseURL',
        help='Server URL',
        required=False,
        default='https://mastodon.social')      

    args = parser.parse_args()


    base_url=args.baseURL

    topic_name = 'mastodon-topic'
    producer = kafka_m_producer(topic_name)

    splits = base_url.split("//")
    mastodon = Mastodon(access_token = secret_token[splits[1]], api_base_url = base_url)


    mastodon.stream_local(Listener())
    
if __name__ == '__main__':
    main()
