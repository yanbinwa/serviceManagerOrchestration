package yanbinwa.iOrchestration.management;

import static org.junit.Assert.*;

import org.junit.Test;

import redis.clients.jedis.Jedis;
import yanbinwa.common.redis.RedisClient;

public class RedisMonitorManagementImplTest
{

    @Test
    public void test()
    {
        String host = "192.168.56.17";
        int port = 6379;
        int maxtotal = 1;
        int maxIdle = 1;
        long maxWaitTime = -1;
        boolean testOnBorrow = true;
        
        RedisClient redisClient = new RedisClient(host, port, maxtotal, maxIdle, maxWaitTime, testOnBorrow);
        Jedis redis = null;
        try
        {
            redis = redisClient.getJedisConnection();
        }
        catch(Exception e)
        {
            e.printStackTrace();
            fail("There is some exception");
        }
        redisClient.setString(redis, "redisTest", "Test");
        String value = redisClient.getString(redis, "redisTest");
        if (!value.equals("Test"))
        {
            fail("redis is not work well");
        }
    }
}
