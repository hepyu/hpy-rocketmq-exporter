from prometheus_client import Gauge
from prometheus_client.core import CollectorRegistry
from flask import Response, Flask

import prometheus_client
import rocketmq_service
import random
import os

app = Flask(__name__)

@app.route("/metrics")
def r_value():
    REGISTRY = CollectorRegistry(auto_describe=False)
    gauge_msg_diff_detail = Gauge("msg_diff_detail", "message count that not be consumed.", ['topic', 'consumerGroup', 'broker', 'queueId', 'consumerClientIP', 'consumerClientPID'], registry=REGISTRY)
    gauge_msg_diff_topic = Gauge("msg_diff_topic", "message count that not be consumed.", ['topic'], registry=REGISTRY)
    gauge_msg_diff_consumergroup = Gauge("msg_diff_consumergroup", "message count that not be consumed.", ['consumerGroup'], registry=REGISTRY)
    gauge_msg_diff_topic_consumergroup = Gauge("msg_diff_topic_consumergroup", "message count that not be consumed.", ['topic', 'consumerGroup'], registry=REGISTRY)
    gauge_msg_diff_broker = Gauge("msg_diff_broker", "message count that not be consumed.", ['broker'], registry=REGISTRY)
    gauge_msg_diff_queue = Gauge("msg_diff_queue", "message count that not be consumed.", ['broker', 'queueId'], registry=REGISTRY)
    gauge_msg_diff_clientinfo = Gauge("msg_diff_clientinfo", "message count that not be consumed.", ['consumerClientIP', 'consumerClientPID'], registry=REGISTRY)

    env_dist = os.environ
    rocketmq_console = env_dist.get('ROCKETMQ_CONSOLE')

    #msg_diff = rocketmq_service.msg_diff('rocketmq-console-ip:7000')
    msg_diff = rocketmq_service.msg_diff(rocketmq_console)
    #msg_diff = rocketmq_service.msg_diff('172.16.13.168:7000')

    msg_diff_details = msg_diff['msg_diff_details']
    for index, info in enumerate(msg_diff_details):
        gauge_msg_diff_detail.labels(info['topic'], info['consumerGroup'], info['broker'], info['queueId'], info['consumerClientIP'], info['consumerClientPID']).set(int(info['diff']))

    msg_diff_topics = msg_diff['msg_diff_topics']
    list_topic_name = msg_diff_topics.keys()
    for index, topic_name in enumerate(list_topic_name):
        gauge_msg_diff_topic.labels(msg_diff_topics[topic_name]['topic']).set(int(msg_diff_topics[topic_name]['diff']))

    msg_diff_consumergroups = msg_diff['msg_diff_consumergroups']
    list_cg_name = msg_diff_consumergroups.keys()
    for index, cg_name in enumerate(list_cg_name):
        gauge_msg_diff_consumergroup.labels(msg_diff_consumergroups[cg_name]['consumerGroup']).set(int(msg_diff_consumergroups[cg_name]['diff']))

    msg_diff_topics_consumergroups = msg_diff['msg_diff_topics_consumergroups']
    list_topic_consumergroup_name = msg_diff_topics_consumergroups.keys()
    for index, topic_consumergroup_name in enumerate(list_topic_consumergroup_name):
        gauge_msg_diff_topic_consumergroup.labels(msg_diff_topics_consumergroups[topic_consumergroup_name]['topic'], msg_diff_topics_consumergroups[topic_consumergroup_name]['consumerGroup']).set(int(msg_diff_topics_consumergroups[topic_consumergroup_name]['diff']))
    
    msg_diff_brokers = msg_diff['msg_diff_brokers']
    list_broker_name = msg_diff_brokers.keys()
    for index, broker_name in enumerate(list_broker_name):
        gauge_msg_diff_broker.labels(msg_diff_brokers[broker_name]['broker']).set(int(msg_diff_brokers[broker_name]['diff']))

    msg_diff_queues = msg_diff['msg_diff_queues']
    list_queue_name = msg_diff_queues.keys()
    for index, queue_name in enumerate(list_queue_name):
        gauge_msg_diff_queue.labels(msg_diff_queues[queue_name]['broker'], msg_diff_queues[queue_name]['queueId']).set(int(msg_diff_queues[queue_name]['diff']))

    msg_diff_clientinfos = msg_diff['msg_diff_clientinfos']
    list_client_name = msg_diff_clientinfos.keys()
    for index, client_name in enumerate(list_client_name):
        gauge_msg_diff_clientinfo.labels(msg_diff_clientinfos[client_name]['consumerClientIP'], msg_diff_clientinfos[client_name]['consumerClientPID']).set(int(msg_diff_clientinfos[client_name]['diff']))


    res = prometheus_client.generate_latest(gauge_msg_diff_detail) + bytes("\n", encoding = "utf8") \
            + prometheus_client.generate_latest(gauge_msg_diff_topic) + bytes("\n", encoding = "utf8") \
            + prometheus_client.generate_latest(gauge_msg_diff_consumergroup) + bytes("\n", encoding = "utf8") \
            + prometheus_client.generate_latest(gauge_msg_diff_topic_consumergroup) + bytes("\n", encoding = "utf8") \
            + prometheus_client.generate_latest(gauge_msg_diff_broker) + bytes("\n", encoding = "utf8") \
            + prometheus_client.generate_latest(gauge_msg_diff_queue) + bytes("\n", encoding = "utf8") \
            + prometheus_client.generate_latest(gauge_msg_diff_clientinfo) + bytes("\n", encoding = "utf8")

    return Response(res, mimetype="text/plain")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9104)
