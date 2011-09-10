Mule Redis Connector
====================

Provides Redis 2.x connectivity to Mule.

> Currently Under Heavy Development!

Pending Envisioned Features
---------------------------

- Support for reading and writing data:
  - Polling inbound endpoint
  - Requester
  - Outbound endpoint
- Support for publish/subscribe:
  - Listening inbound endpoint
  - Dispatching outbound endpoint

Build Commands
--------------

To compile and install in the local Maven repository:

    mvn clean install  

If you have Redis running on localhost:6379, you can run the integration tests with:

    mvn -Pit verify


Features
--------

### Configuration

Connecting to a local Redis with no password and default connection pooling is as simple as:

    <redis:config name="localRedis" />

The following demonstrates all the possible configuration options:

    <spring:beans>
        <spring:bean name="redisPoolConfiguration"
                     class="redis.clients.jedis.JedisPoolConfig"
                     p:whenExhaustedAction="#{T(org.apache.commons.pool.impl.GenericObjectPool).WHEN_EXHAUSTED_GROW}" />
    </spring:beans>
    
    <redis:config name="localRedis"
                  host="localhost"
                  port="6379"
                  password="s3cre3t"
                  timeout="15000"
                  poolConfig-ref="redisPoolConfiguration" />

### ObjectStore

The configured Redis module can act as an [ObjectStore](http://www.mulesoft.org/docs/site/current3/apidocs/index.html?org/mule/api/store/ObjectStore.html), which can be injected into any object needing such a store:

The following shows how to use the Redis module as the data store for a Mule-powered [PubSubHubbub hub](https://github.com/mulesoft/mule-module-pubsubhubbub):

    <redis:config name="localRedis" />
    <pubsubhubbub:config objectStore-ref="localRedis" />
