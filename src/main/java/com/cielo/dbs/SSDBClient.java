package com.cielo.dbs;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.nutz.ssdb4j.SSDBs;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDB;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


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

    private String filtrateKey(Object key) {
        if (key instanceof Integer) {
            return String.format("%010d", key);
        } else if (key instanceof Long) {
            return String.format("%019d", key);
        }
        return (String) key;
    }

    //设置一个基本类型值
    public Response set(Object key, Object val) {
        return ssdb.set(filtrateKey(key), val);
    }

    public Response get(Object key) {
        return ssdb.get(filtrateKey(key));
    }

    public Response scan(Object fromKey, Object endKey) {
        return ssdb.scan(filtrateKey(fromKey), filtrateKey(endKey), scanNum);
    }

    public Map<String, String> scanMapString(Object fromKey, Object endKey) {
        return scan(fromKey, endKey).mapString();
    }

    public Map<String, byte[]> scanBytes(Object fromKey, Object endKey) {
        return scan(fromKey, endKey).map().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> (byte[]) e.getValue(), (a, b) -> b));
    }

    public Map<String, String> scanMapString(Object prefix) {
        return scanMapString(prefix, prefix + "}");
    }

    public List<String> scanKeys(Object prefix) {
        return scanKeys(prefix, prefix + "}");
    }

    public List<String> scanKeys(Object fromKey, Object endKey) {
        return ssdb.keys(filtrateKey(fromKey), filtrateKey(endKey), scanNum).listString();
    }

    public Response expire(Object key, int ttl) {
        return ssdb.expire(filtrateKey(key), ttl);
    }

    public Response del(Object key) {
        return ssdb.del(filtrateKey(key));
    }

    public boolean exists(Object key) {
        return ssdb.exists(filtrateKey(key)).asInt() != 0;
    }

    public void flushDB() {
        ssdb.flushdb(null);
    }
}
