package org.mule.module.redis;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.mule.api.annotations.Configurable;
import org.mule.api.annotations.Module;
import org.mule.api.annotations.param.Default;
import org.mule.api.annotations.param.Optional;

import redis.clients.jedis.JedisPool;

@Module(name = "redis", namespace = "http://www.mulesoft.org/schema/mule/redis", schemaLocation = "http://www.mulesoft.org/schema/mule/redis/3.2/mule-redis.xsd")
public class RedisModule {
    protected static final Log LOGGER = LogFactory.getLog(RedisModule.class);

    @Configurable
    @Optional
    @Default("localhost")
    private String host;

    @Configurable
    @Optional
    @Default("6379")
    private int port;

    @Configurable
    @Optional
    @Default("2000")
    private int timeout;

    @Configurable
    @Optional
    private String password;

    @Configurable
    @Optional
    private Config poolConfig = new Config();

    private JedisPool jedisPool;

    @PostConstruct
    public void initializeJedis() {
        jedisPool = new JedisPool(poolConfig, host, port, timeout, password);

        LOGGER.info(String.format("Redis connector ready, host: %s, port: %d, timeout: %d, password: %s, pool config: %s", host, port,
                timeout, StringUtils.repeat("*", StringUtils.length(password)),
                ToStringBuilder.reflectionToString(poolConfig, ToStringStyle.SHORT_PREFIX_STYLE)));
    }

    @PreDestroy
    public void destroyJedis() {
        jedisPool.destroy();
        LOGGER.info("Redis connector terminated");
    }

    public String getHost() {
        return host;
    }

    public void setHost(final String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(final int port) {
        this.port = port;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(final int timeout) {
        this.timeout = timeout;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(final String password) {
        this.password = password;
    }

    public Config getPoolConfig() {
        return poolConfig;
    }

    public void setPoolConfig(final Config poolConfig) {
        this.poolConfig = poolConfig;
    }
}
