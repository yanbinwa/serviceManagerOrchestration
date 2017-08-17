package yanbinwa.iOrchestration.management;

import static org.junit.Assert.*;

import org.junit.Test;

import yanbinwa.common.redis.RedisClient;
import yanbinwa.iOrchestration.constants.OrchestrationConstants;

public class RedisMonitorManagementImplTest
{

    @Test
    public void test()
    {
        String host = OrchestrationConstants.TEST_SERVER_IP;
        int port = OrchestrationConstants.TEST_REDIS_PORT;
        int maxtotal = 1;
        int maxIdle = 1;
        long maxWaitTime = -1;
        boolean testOnBorrow = true;
        
        RedisClient redisClient = new RedisClient(host, port, maxtotal, maxIdle, maxWaitTime, testOnBorrow);
        try
        {
            boolean ret = redisClient.getJedisConnection();
            if (!ret)
            {
                fail("Can not build connection");
            }
            redisClient.setString("redisTest", "Test");
            String value = redisClient.getString("redisTest");
            if (!value.equals("Test"))
            {
                fail("redis is not work well");
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
            fail("There is some exception");
        }
        finally
        {
            redisClient.returnJedisConnection();
        }
        redisClient.closePool();
    }
}
