package com.xiaoan.redis.redis2hive;

/**
 * Created by tanjie on 4/27/16.
 */
public class App {
    public static void main(String[] args) {
        Thread thread = new Thread(new Transform());
        thread.start();
    }
}
