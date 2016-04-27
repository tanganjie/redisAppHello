package com.xiaoan.redis.redis2hive;

import com.xiaoan.redis.common.HdfsClient;
import com.xiaoan.redis.common.HdfsConfigure;
import com.xiaoan.redis.common.RedisClient;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by tanjie on 4/27/16.
 */
public class Transform implements Runnable {

    static Logger log = Logger.getLogger(Transform.class);

    private static final long sleeptime = 120 * 1000;

    public OutputStream getOutPutStream(String filename) throws IOException {
        HdfsClient hdfsClient = HdfsClient.getInstance();
        return hdfsClient.getFs().create(new Path(HdfsConfigure.hdfsUri + HdfsConfigure.path + filename));
    }

    public void run() {
        Map<String, Double> map = null;
        while(true) {
            OutputStream out = null;
            try {
                long time = System.currentTimeMillis();
                map = new HashMap<String, Double>();
                Jedis jedis = RedisClient.getJedis();

                //读redis,取所有key
                Set<String> keySet = jedis.keys("*");
                log.info("keys num: " + keySet.size());

                //处理redis数据
                Iterator<String> iterator = keySet.iterator();
                while (iterator.hasNext()) {
                    String key = iterator.next();
                    String value = jedis.get(key);
                    String[] id_cash = value.split("|");
                    if (id_cash.length != 2) {
                        log.info("key:" + key + "; value:" + value + "   not valid");
                        continue;
                    }
                    if (map.containsKey(id_cash[0])) {
                        double cash = map.get(id_cash[0]);
                        try {
                            cash += Double.parseDouble(id_cash[0]);
                        } catch (Exception e) {
                            e.printStackTrace();
                            log.error("cash can not be transformed to double");
                            continue;
                        }
                        map.put(id_cash[0], cash);
                    }
                }

                //存hdfs,文件名time
                out = getOutPutStream(time + "");
                Set<Map.Entry<String, Double>> entrySet = map.entrySet();
                Iterator<Map.Entry<String, Double>> iter = entrySet.iterator();
                int n = 0;
                while (iter.hasNext()) {
                    Map.Entry<String, Double> entry = iter.next();
                    String line = entry.getKey() + "\t" + entry.getValue();
                    out.write(line.getBytes());
                    n++;
                    if(n == 100) {
                        out.flush();
                        n = 0;
                    }
                }

                map = null;
                Thread.sleep(sleeptime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    out.flush();
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
