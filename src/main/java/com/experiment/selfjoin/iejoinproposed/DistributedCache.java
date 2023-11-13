package com.experiment.selfjoin.iejoinproposed;

import redis.clients.jedis.Jedis;

public class DistributedCache {
    private Jedis jedis;
    public DistributedCache(String hostName, int port){
        jedis= new Jedis(hostName, port);
    }
    public void setValue(String key, int value){
        jedis.set(key, String.valueOf(value));
    }
    public int getValue(String key){
        String value= jedis.get(key);
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                // Handle the case where the stored value is not a valid integer
                // You can log an error, throw an exception, or use a default value as needed.
                return -1; // Default value
            }
        } else {
            // Handle the case where the key is not found in Redis
            return -1; // Default value
        }
    }
    public boolean keyExists(String key) {
        return jedis.exists(key);
    }

    // Other methods to interact with Redis

    public void close() {
        jedis.close();
    }
}
