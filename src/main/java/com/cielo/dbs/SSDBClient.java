package com.cielo.dbs;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.nutz.ssdb4j.SSDBs;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDB;

import java.util.List;
import java.util.Map;


public class SSDBClient {
    private int scanNum = 1000;
    private SSDB ssdb;

    public SSDBClient(int scanNum) {
        ssdb = SSDBs.pool("ws.cielosun.xyz", 32768, 10000, genericObjectPoolConfig());
        this.scanNum = scanNum;
    }

    private GenericObjectPoolConfig genericObjectPoolConfig() {
        //配置线程池
        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxWaitMillis(5000);
        genericObjectPoolConfig.setNumTestsPerEvictionRun(100);
        genericObjectPoolConfig.setSoftMinEvictableIdleTimeMillis(10000);
        genericObjectPoolConfig.setTimeBetweenEvictionRunsMillis(5000);
        genericObjectPoolConfig.setTestOnBorrow(true);
        genericObjectPoolConfig.setTestOnReturn(true);
        genericObjectPoolConfig.setTestWhileIdle(true);
        genericObjectPoolConfig.setLifo(true);
        return genericObjectPoolConfig;
    }

    //设置一个基本类型值
    public Response set(Object key, Object val) {
        return ssdb.set(key, val);
    }

    public Response get(Object key) {
        return ssdb.get(key);
    }

    public Map<String, String> scan(Object fromKey, Object endKey) {
        return ssdb.scan(fromKey, endKey, scanNum).mapString();
    }

    public Map<String, String> scan(Object prefix) {
        return scan(prefix, prefix + "}");
    }

    public List<String> scanKeys(Object prefix) {
        return scanKeys(prefix, prefix + "}");
    }

    public List<String> scanKeys(Object fromKey, Object endKey) {
        return ssdb.keys(fromKey, endKey, scanNum).listString();
    }

    public int count(Object prefix) {
        return scan(prefix).size();
    }

    public Response expire(Object key, int ttl) {
        return ssdb.expire(key, ttl);
    }

    public Response del(Object key) {
        return ssdb.del(key);
    }

    public boolean exists(Object key) {
        return ssdb.exists(key).asInt() != 0;
    }

    public Response lowerBound(Object key) {
        return ssdb.scan(key, "", 1);
    }

    public Response lowerBoundKey(Object key) {
        return ssdb.keys(key, "", 1);
    }

    public String lowerBoundVal(Object key) {
        return lowerBound(key).mapString().values().iterator().next();
    }

    public void flushDB() {
        ssdb.flushdb(null);
    }
}
