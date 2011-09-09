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
- Implementation of [ObjectStore](http://www.mulesoft.org/docs/site/current3/apidocs/index.html?org/mule/api/store/ObjectStore.html) to offer Mule+Redis users a solid ObjectStore implementation (used as a data store for many Mule components).


Build Commands
--------------

To compile and install in the local Maven repository:

    mvn clean install  

If you have Redis running on localhost:6379, you can run the integration tests with:

    mvn -Pit verify


Features
--------

### ObjectStore

The configured Redis module can act as an [ObjectStore](http://www.mulesoft.org/docs/site/current3/apidocs/index.html?org/mule/api/store/ObjectStore.html).
As such it can be injected into any bean that needs such a store:

    <spring:beans>
        <spring:bean class="..." p:objectStore-ref="localRedis" />
    </spring:beans>

    <redis:config name="localRedis" />
