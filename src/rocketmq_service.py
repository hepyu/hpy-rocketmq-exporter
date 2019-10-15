import pycurl
import json
import os
import sys

import rocketmq_dao
import rocketmq_constant

from prometheus_client import Summary

#rocketmq-console-ip:7000
#rocketmq_console_ip_port = sys.argv[1]
#ignore topics
#list_stop_topic_name = ['RMQ_SYS_TRANS_HALF_TOPIC','BenchmarkTest','OFFSET_MOVED_EVENT','TBW102','SELF_TEST_TOPIC','DefaultCluster','broker-b','broker-a']

def msg_diff(rocketmq_console_ip_port):

    list_topic_name = rocketmq_dao.getTopicNameList(rocketmq_console_ip_port)
    if list_topic_name is None:
        return None
    
    rt = {}
    diff_detail_array = []
    diff_topics = {}
    diff_consumergroups = {}
    diff_topics_consumergroups = {}
    diff_brokers = {}
    diff_clientinfoes = {}
    diff_queues = {}


    for index, topic_name in enumerate(list_topic_name):
        
        if ("benchmark_consumer" in topic_name) or (topic_name in rocketmq_constant.list_stop_topic_name) or ('%RETRY%' in topic_name):
            continue

        data = rocketmq_dao.getConsumerByTopic(rocketmq_console_ip_port, topic_name)
        if data is None:
            continue

        list_consumer_group = data.keys()
        for m, consumer_group in enumerate(list_consumer_group):
            data_consumer_group = data[consumer_group]
            topic = data_consumer_group['topic']
            diffTotal = data_consumer_group['diffTotal']
            lastTimestamp = data_consumer_group['lastTimestamp']
            queueStatInfoList = data_consumer_group['queueStatInfoList']
      
            for n, queue in enumerate(queueStatInfoList):
                diff_detail = {}

                brokerName = queue['brokerName']
                queueId = queue['queueId']

                clientInfo = queue['clientInfo']
                temp_array = clientInfo.split('@')
                consumerClientIP = temp_array[0]
                consumerClientPID = temp_array[1]

                diff = int(queue['brokerOffset'] - queue['consumerOffset'])
                lastTimestamp = queue['lastTimestamp']

                diff_detail['broker'] = brokerName
                diff_detail['queueId'] = queueId
                #diff_detail['clientInfo'] = clientInfo
                diff_detail['consumerClientIP'] = consumerClientIP
                diff_detail['consumerClientPID'] = consumerClientPID
                diff_detail['diff'] = diff
                diff_detail['topic'] = topic
                diff_detail['consumerGroup'] = consumer_group
                diff_detail_array.append(diff_detail)

                if topic in diff_topics:
                    diff_topics[topic]['diff'] = diff_topics[topic]['diff'] + diff
                else:
                    diff_topics[topic] = {}
                    diff_topics[topic]['topic'] = topic
                    diff_topics[topic]['diff'] = diff

                if consumer_group in diff_consumergroups:
                    diff_consumergroups[consumer_group]['diff'] = diff_consumergroups[consumer_group]['diff'] + diff
                else:
                    diff_consumergroups[consumer_group] = {}
                    diff_consumergroups[consumer_group]['consumerGroup'] = consumer_group
                    diff_consumergroups[consumer_group]['diff'] = diff

                topic_consumergroup_name = topic+":"+consumer_group
                if topic_consumergroup_name in diff_topics_consumergroups:
                    diff_topics_consumergroups[topic_consumergroup_name]['diff'] = diff_topics_consumergroups[topic_consumergroup_name]['diff'] + diff
                else:
                    diff_topics_consumergroups[topic_consumergroup_name] = {}
                    diff_topics_consumergroups[topic_consumergroup_name]['topic'] = topic
                    diff_topics_consumergroups[topic_consumergroup_name]['consumerGroup'] = consumer_group
                    diff_topics_consumergroups[topic_consumergroup_name]['diff'] = diff

                if brokerName in diff_brokers:
                    diff_brokers[brokerName]['diff'] = diff_brokers[brokerName]['diff'] + diff
                else:
                    diff_brokers[brokerName] = {}
                    diff_brokers[brokerName]['broker'] = brokerName
                    diff_brokers[brokerName]['diff'] = diff

                queuestr = str(brokerName) + ":" + str(queueId)
                if queuestr in diff_queues:
                    diff_queues[queuestr]['diff'] = diff_queues[queuestr]['diff'] + diff
                else:
                    diff_queues[queuestr] = {}
                    diff_queues[queuestr]['broker'] = brokerName
                    diff_queues[queuestr]['queueId'] = queueId
                    diff_queues[queuestr]['diff'] = diff

                if clientInfo in diff_clientinfoes:
                    diff_clientinfoes[clientInfo]['diff'] = diff_clientinfoes[clientInfo]['diff'] + diff
                else:
                    diff_clientinfoes[clientInfo] = {}
                    diff_clientinfoes[clientInfo]['consumerClientIP'] = consumerClientIP
                    diff_clientinfoes[clientInfo]['consumerClientPID'] = consumerClientPID
                    diff_clientinfoes[clientInfo]['diff'] = diff

    rt['msg_diff_details'] = diff_detail_array

    rt['msg_diff_topics'] = diff_topics
    rt['msg_diff_consumergroups'] = diff_consumergroups
    rt['msg_diff_topics_consumergroups'] = diff_topics_consumergroups
    rt['msg_diff_brokers'] = diff_brokers
    rt['msg_diff_queues'] = diff_queues 
    rt['msg_diff_clientinfos'] = diff_clientinfoes 

    return rt
