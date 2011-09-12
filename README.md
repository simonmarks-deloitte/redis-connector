Mule Redis Connector
====================

> Currently Under Heavy Development!

Provides Redis connectivity to Mule:

- Supports Redis Publish/Subscribe model for asynchronous message exchanges,
- Allows direct reading and writing operations in Redis collections,  
- Allows using Redis as a datastore for Mule components that require persistence (like the [http://www.mulesoft.org/documentation/display/MULE3USER/Routing+Message+Processors#RoutingMessageProcessors-IdempotentMessageFilter](Idempotent Message Filter)).

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

The configured Redis module can act as an [ObjectStore](http://www.mulesoft.org/docs/site/current3/apidocs/index.html?org/mule/api/store/ObjectStore.html), which can be injected into any object needing such a store.

> Mule object stores are stored as Redis Hashes named "mule.objectstore.{ospn}", where ospn is the object store partition name (or "_default" if none has been specified).

For example, the following shows how to use the Redis module as the data store for a Mule-powered [PubSubHubbub hub](https://github.com/mulesoft/mule-module-pubsubhubbub):

    <redis:config name="localRedis" />
    <pubsubhubbub:config objectStore-ref="localRedis" />
