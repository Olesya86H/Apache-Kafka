#!/usr/bin/env python
# coding: utf-8

# In[6]:


from time import sleep
from json import dumps
from kafka import KafkaProducer


# In[7]:


#задаем отправителя сообщений:
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'),
                         api_version=(0, 10, 1),
                         security_protocol='PLAINTEXT')


# In[8]:


from kafka.admin import KafkaAdminClient, NewTopic
#задаем API администратора для формирования топика и партицирования топика: 
#два получателя - два раздела одного топика - каждому получателю свой раздел (по одному сообщению каждому)
admin = KafkaAdminClient(
        client_id ='admin',
        bootstrap_servers=['localhost:9092'],
        security_protocol='PLAINTEXT',
    )

topic_name = "first_partitioned_topic_via_kafka"

topic = NewTopic(name=topic_name, num_partitions=2, replication_factor=1)

admin.create_topics([topic])


# In[9]:


#первый раздел топика:
producer.send(topic_name, value={'the first  part of topic for consumer1' : 1}, partition=0)

#второй раздел топика:
producer.send(topic_name, value={'the second part of topic for consumer2' : 2}, partition=1)

#блокировка отправителя, пока сообщения не будут доставлены:
#producer.flush()


# In[ ]:




