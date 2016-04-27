package com.xiaoan.redis.common;

import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by tanjie on 4/27/16.
 */
public class RedisClient {
    static Logger log = Logger.getLogger(RedisClient.class);
    private static Jedis jedis = null;
    private static Properties properties = null;

    private RedisClient() {}

    public static Jedis getJedis() {
        if (properties == null) {
            InputStream in = RedisClient.class.getResourceAsStream("/redis.properties");
            properties = new Properties();
            try {
                properties.load(in);
            } catch (IOException e) {
                e.printStackTrace();
                log.error("redis config error!");
                System.exit(-1);
            }
        }
        if (jedis == null) {
            String url = properties.getProperty("redis.url");
            String port = properties.getProperty("redis.port");
            //String password = properties.getProperty("redis.password");
            try {
                jedis = new Jedis(url, Integer.parseInt(port));
                //jedis.auth(password);
            } catch (Exception e) {
                e.printStackTrace();
                log.error("redis connect error!");
                System.exit(1);
            }
        }
        return jedis;
    }
}
