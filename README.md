Mule Redis Connector
====================

> Currently Under Heavy Development!

Provides Redis connectivity to Mule:

- Supports [Redis Publish/Subscribe model](http://redis.io/topics/pubsub) for asynchronous message exchanges,
- Allows direct reading and writing operations in Redis collections,  
- Allows using Redis as a datastore for Mule components that require persistence (like the [Idempotent Message Filter](http://www.mulesoft.org/documentation/display/MULE3USER/Routing+Message+Processors#RoutingMessageProcessors-IdempotentMessageFilter)).

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

    <redis:config />
    
If you need to refer to this configuration (in case you have several different connections to different Redis servers or if you need to inject it), then you'll have to name it:

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
                  connectionTimeout="15000"
                  poolConfig-ref="redisPoolConfiguration" />


### Datastructure Operations

This module allows your Mule flows to interact with the main Redis datastructures: [strings](http://redis.io/commands#string, [hashes](http://redis.io/commands#hash), lists, sets and sorted sets.

> You must have at least one redis:config element, see above.

#### Strings

Storing the current payload under the specified key can be done with different options:

    <redis:set key="my_key" />
    <redis:set key="my_key" expire="3600" />
    <redis:set key="my_key" ifNotExists="true" />

Retrieving is done with:

    <redis:get key="my_key" />

#### Strings

Storing the current payload under the specified key and field can be done with different options:

    <redis:hash-set key="my_key" field="my_field" />
    <redis:hash-set key="my_key" field="my_field" ifNotExists="true" />

Retrieving is done with:

    <redis:hash-get key="my_key" field="my_field" />

### Publish/Subscribe

> You must have at least one redis:config element, see above.

Publishing to a Redis channel is achieved as shown here after:

    <redis:publish channel="news.art.figurative" />

Any message hitting this [message processor](http://www.mulesoft.org/documentation/display/MULE3USER/Message+Sources+and+Message+Processors#MessageSourcesandMessageProcessors-MessageProcessors) will be transformed into a byte array (using Mule's transformation infrastructure) and will be published to the "news.art.figurative" channel.

Subscribing to a channel is done by specifying names or patterns to which Mule will listen. For example, the following subscribes to a channel named "news.sport.hockey" and any channel that matches the "news.art.*" globbing pattern.

    <redis:subscribe>
        <redis:channels>
            <redis:channel>news.sport.hockey</redis:channel>
            <redis:channel>news.art.*</redis:channel>
        </redis:channels>
    </redis:subscribe>

This can be used as a [message source](http://www.mulesoft.org/documentation/display/MULE3USER/Message+Sources+and+Message+Processors#MessageSourcesandMessageProcessors-MessageSources) in any flow. 


### Object Store

> You must have at least one redis:config element, see above.

The configured Redis module can act as an [ObjectStore](http://www.mulesoft.org/docs/site/current3/apidocs/index.html?org/mule/api/store/ObjectStore.html), which can be injected into any object needing such a store.

> Mule object stores are stored as Redis Hashes named "mule.objectstore.{ospn}", where ospn is the object store partition name (or "_default" if none has been specified).

For example, the following shows how to use the Redis module as the data store for a Mule-powered [PubSubHubbub hub](https://github.com/mulesoft/mule-module-pubsubhubbub):

    <redis:config name="localRedis" />
    <pubsubhubbub:config objectStore-ref="localRedis" />
