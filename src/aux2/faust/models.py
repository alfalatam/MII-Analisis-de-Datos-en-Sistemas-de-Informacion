from faust import Record
from confluent_kafka import avro

class Toot(Record):
    _schema = avro.load("./config/avro/mastodon-topic-value.avsc")
