# Stream Registry python client for Kafka

[![Version](https://img.shields.io/pypi/v/stream_registry_python_client.svg)](https://img.shields.io/pypi/v/stream_registry_python_client.svg)
[![License](https://img.shields.io/hexpm/l/plug.svg)](https://img.shields.io/hexpm/l/plug.svg)

[![Build Status](https://travis-ci.org/homeaway/stream-registry-python-client.svg?branch=master)](https://travis-ci.org/homeaway/stream-registry-python-client)
[![Python versions](https://img.shields.io/pypi/pyversions/stream_registry_python_client.svg)](https://www.python.org/)

[![Python versions](https://img.shields.io/pypi/pyversions/stream_registry_python_client.svg)](https://www.python.org/)

Welcome to the stream registry client for python. This is a client side library to the [stream registry](https://homeaway.github.io/stream-registry/). Its goal is to help bootstrap a [Confluent kafka](https://github.com/confluentinc/confluent-kafka-python) producer or consumer utilizing the registry. With this approach, the user is no longer required to understand the physical topology of a 'stream' and instead focus on the data that is produced or consumed.

## Using the client

For more comprehensive use of the Confluent Kafka client please check [the github repository](https://github.com/confluentinc/confluent-kafka-python) or [the docs](http://docs.confluent.io/current/clients/confluent-kafka-python/index.html).

### Pre-requisites

The Confluent Kafka client requires the use of the [librdkafka](https://github.com/edenhill/librdkafka) which must be installed in the running environment (including any containerized one).

### Consuming

There are two options to consume, either use the high level Kakfa consumer or leverage the Confluent Avro one. 

To initialize a simple high level consumer:

```python
import stream_registry_python_client.consumer.builder as builder

registry_config = {
                    'base_url': 'http://myregistry.mydomain.com',
                    'region': 'us-east-1',
                    'app_name': 'mysampleapp'
                   }
consumer, topics = builder.create_consumer(registry_config=self.registry_config,
                                           stream_name=self.stream_name,
                                           avro_consumer=False, 
                                           kafka_properties={'auto.offset.reset': 'latest'})
""" Start consuming """
```

and to use the AVRO one:

```python
import stream_registry_python_client.consumer.builder as builder

registry_config = {
                    'base_url': 'http://myregistry.mydomain.com',
                    'region': 'us-east-1',
                    'app_name': 'mysampleapp'
                   }
consumer, topics = builder.create_consumer(registry_config=self.registry_config,
                                           stream_name=self.stream_name,
                                           avro_consumer=True, 
                                           kafka_properties={'auto.offset.reset': 'latest'})
""" Start consuming """
```

When the 'create_consumer' function is called, by default the cosumer will be subscribed to all the topics indicated by the stream registry. If you would like to override this behavior and perform the subscription at a later time pass the property auto_subscribe=False and use the returned list of topics to subscribe as in the example below:


```python
import stream_registry_python_client.consumer.builder as builder

registry_config = {
                    'base_url': 'http://myregistry.mydomain.com',
                    'region': 'us-east-1',
                    'app_name': 'mysampleapp'
                   }
consumer, topics = builder.create_consumer(registry_config=registry_config,
                                           stream_name='TestStream',
                                           avro_consumer=True, 
                                           auto_subscribe=False,
                                           kafka_properties={'auto.offset.reset': 'latest'})
"""Some other thing happening here"""
                                   
consumer.subscribe(topics)
""" Start consuming """
```

### Producing

Producing with a simple high level client is very similar to consuming, the only difference is that you will have to keep the topic available to indicate where the production happens:

```python
import stream_registry_python_client.consumer.builder as builder

registry_config = {
                    'base_url': 'http://myregistry.mydomain.com',
                    'region': 'us-east-1',
                    'app_name': 'mysampleapp'
                   }

producer, topic = builder.create_producer(registry_config=registry_config, 
                                          stream_name='TestStream')
producer.produce(topic, 'helloworld this is python'.encode('utf-8'), callback=callback)
producer.flush()


def callback(self, err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
```

Producing with AVRO is a little bit more involved, you WILL have to pass a the avsc strings so the underlying encoder can generate the proper encoding.

```python
import stream_registry_python_client.consumer.builder as builder

registry_config = {
                    'base_url': 'http://myregistry.mydomain.com',
                    'region': 'us-east-1',
                    'app_name': 'mysampleapp'
                   }

value_schema_str = """
{
   "namespace": "my.test",
   "name": "value",
   "type": "record",
   "fields" : [
     {
       "name" : "name",
       "type" : "string"
     }
   ]
}
"""

key_schema_str = """
{
   "namespace": "my.test",
   "name": "key",
   "type": "record",
   "fields" : [
     {
       "name" : "name",
       "type" : "string"
     }
   ]
}
"""

producer, topic = builder.create_avro_producer(registry_config=registry_config, 
                                                stream_name='TestStream',
                                                key_schema_str=key_schema_str, 
                                                value_schema_str= value_schema_str)
producer.produce(topic='my_topic', key={"name", "Key"}, value={"name": "Value"}, callback=callback)
producer.flush()


def callback(self, err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
```

## Developing

### Building

The recommended way to build this library is with the use of tox. The makefile and tox.ini file are configured to create a tox virtual environment that can build the library.

For IDEs you can use [virtualenv] (https://virtualenv.pypa.io/en/latest/) to create the environment and point the IDE to it. This has been tested with PyCharm and VSCode.

### Contributions

Contributions are always welcomed, please follow the [Contributing.md](CONTRIBUTING.md) guidelines and the [code of conduct](CODE_OF_CONDUCT.md). Create a gihub issue and submit a PR that complies with the quality standards and we'll take care of the rest!
