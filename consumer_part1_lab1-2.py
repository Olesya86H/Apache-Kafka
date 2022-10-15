#!/usr/bin/env python
# coding: utf-8

# In[1]:


from kafka import TopicPartition
from kafka import KafkaConsumer
import json


# In[2]:


#задаем второго получателя:
topic_name = "first_partitioned_topic_via_kafka"
consumer = KafkaConsumer(
     group_id="topic_partitioned",
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     api_version=(0, 10, 1),
     security_protocol='PLAINTEXT')


# In[ ]:


#привязываем второго получателя к топику (получает второй раздел топика, 2-е сообщение):
consumer.assign([TopicPartition(topic_name, 1)])
consumer.subscription()
for message in consumer:
    print ("p=%d value=%s" % (message.partition, message.value))


# In[ ]:




