package com.xiaoan.redis.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;

/**
 * Created by tanjie on 4/27/16.
 */
public class HdfsClient {
    static Logger log = Logger.getLogger(HdfsClient.class);

    private static HdfsClient hdfsClient = null;

    private FileSystem fs;

    private Configuration conf;

    private HdfsClient(String uri) {
        try {
            conf = new Configuration();
            fs = FileSystem.get(URI.create(uri), conf);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("can't connect to hdfs, program will exit", e);
            System.exit(-1);
        }
    }

    public static HdfsClient getInstance() {
        if (hdfsClient == null) {
            hdfsClient = new HdfsClient(HdfsConfigure.hdfsUri); //hdfspath
        }
        return hdfsClient;
    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public FileSystem getFs() {
        return fs;
    }

    public void setFs(FileSystem fs) {
        this.fs = fs;
    }
}
