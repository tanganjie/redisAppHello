package com.xiaoan.redis.common;

import org.apache.log4j.Logger;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.Iterator;
import java.util.Set;

/**
 * Created by tanjie on 4/27/16.
 */
public class RedisConnection {
    static Logger log = Logger.getLogger(RedisConnection.class);

    @Test
    public void redisConnection() {
        Jedis jedis = new Jedis("localhost");
        log.info("connected to localhost");
        log.info("server is running " + jedis.ping());
    }

    @Test
    public void redisKeys() {
        Set<String> keysSet = RedisClient.getJedis().keys("*");
        Iterator<String> iterator = keysSet.iterator();
        while(iterator.hasNext()) {
            log.info(iterator.next());
        }
    }
}
