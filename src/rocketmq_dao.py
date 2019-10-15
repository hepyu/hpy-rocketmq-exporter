import pycurl
import json
import os
import sys

import http_util

from prometheus_client import Summary

#rocketmq-console-ip:7000
#rocketmq_console_ip_port = sys.argv[1]
#ignore topics
#list_stop_topic_name = ['RMQ_SYS_TRANS_HALF_TOPIC','BenchmarkTest','OFFSET_MOVED_EVENT','TBW102','SELF_TEST_TOPIC','DefaultCluster','broker-b','broker-a']


def getTopicNameList(rocketmq_console_ip_port):
    url = str('http://%s/topic/list.query' % (str(rocketmq_console_ip_port)))
    data_json = http_util.curl_http_url(url)
    return fetch_result(data_json, 'topicList')


def getConsumerByTopic(rocketmq_console_ip_port, topic_name):
    url = str("http://%s/topic/queryConsumerByTopic.query?topic=%s" % (str(rocketmq_console_ip_port), str(topic_name)))
    data_json = http_util.curl_http_url(url)
    return fetch_result(data_json)

def fetch_result(data_json, key=None):
    if key is None:
        return data_json

    if data_json is None:
        return None
    else:
        return data_json[str(key)]
