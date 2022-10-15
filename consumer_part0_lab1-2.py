#!/usr/bin/env python
# coding: utf-8

# In[1]:


from kafka import TopicPartition
from kafka import KafkaConsumer
import json


# In[2]:


#задаем первого получателя:
topic_name = "first_partitioned_topic_via_kafka"
consumer = KafkaConsumer(
     group_id="topic_partitioned",
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     api_version=(0, 10, 1),
     security_protocol='PLAINTEXT')


# In[ ]:


#привязываем первого получателя к топику (получает первый раздел топика, 1-е сообщение):
consumer.assign([TopicPartition(topic_name, 0)])
consumer.subscription()
for message in consumer:
    print ("p=%d value=%s" % (message.partition, message.value))


# In[ ]:




