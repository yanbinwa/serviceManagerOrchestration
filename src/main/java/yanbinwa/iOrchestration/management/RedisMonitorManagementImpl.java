package yanbinwa.iOrchestration.management;

import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import yanbinwa.common.redis.RedisClient;
import yanbinwa.common.zNodedata.ZNodeServiceData;
import yanbinwa.common.zNodedata.ZNodeServiceDataImpl;
import yanbinwa.iOrchestration.exception.ServiceUnavailableException;
import yanbinwa.iOrchestration.service.OrchestrationService;

/**
 * 通过redisClient.getJedisConnection()方法就可以判断redis是否存活
 * 
 * @author yanbinwa
 *
 */

public class RedisMonitorManagementImpl implements MonitorManagement
{
    private static final Logger logger = Logger.getLogger(RedisMonitorManagementImpl.class);
    
    private OrchestrationService orchestrationService = null;
    private RedisClient redisClient = null;
    
    private String redisHost;
    private int redisPort;
    private int maxTotal;
    private int maxIdle;
    private long maxWait;
    private boolean testOnBorrow;
    
    private Thread monitorThread = null;
    
    private boolean isRunning = false;
    
    ZNodeServiceData redisData = null;
            
    public RedisMonitorManagementImpl(Map<String, String> redisProperites, OrchestrationService orchestrationService)
    {
        this.orchestrationService = orchestrationService;
        
        String serviceGroupName = redisProperites.get(OrchestrationService.SERVICE_SERVICEGROUPNAME);
        if (serviceGroupName == null)
        {
            logger.error("redis service group name should not be null");
            return;
        }
        String serviceName = redisProperites.get(OrchestrationService.SERVICE_SERVICENAME);
        if (serviceName == null)
        {
            logger.error("redis service name should not be null");
            return;
        }
        
        redisData = new ZNodeServiceDataImpl("redisIp", serviceGroupName, serviceName, -1, "redisUrl");
        
        redisHost = redisProperites.get(MonitorManagement.MONITOR_REDIS_HOST_KEY);
        if (redisHost == null)
        {
            logger.error("Redis host should not be null");
            return;
        }
        String redisPortStr = redisProperites.get(MonitorManagement.MONITOR_REDIS_PORT_KEY);
        if (redisPortStr == null)
        {
            logger.error("Redis port should not be null");
            return;
        }
        redisPort = Integer.parseInt(redisPortStr);
        
        maxTotal = MonitorManagement.MONITOR_REDIS_MAX_TOTAL_DEFAULT;
        String maxTotalStr = redisProperites.get(MonitorManagement.MONITOR_REDIS_MAX_TOTAL_KEY);
        if (maxTotalStr != null)
        {
            maxTotal = Integer.parseInt(maxTotalStr);
        }
        
        maxIdle = MonitorManagement.MONITOR_REDIS_MAX_IDEL_DEFAULT;
        String maxIdleStr = redisProperites.get(MonitorManagement.MONITOR_REDIS_MAX_WAIT_KEY);
        if (maxIdleStr != null)
        {
            maxIdle = Integer.parseInt(maxIdleStr);
        }
        
        maxWait = MonitorManagement.MONITOR_REDIS_MAX_WAIT_DEFAULT;
        String maxWaitStr = redisProperites.get(MonitorManagement.MONITOR_REDIS_MAX_WAIT_KEY);
        if (maxWaitStr != null)
        {
            maxWait = Integer.parseInt(maxWaitStr);
        }
        
        testOnBorrow = MonitorManagement.MONITOR_REDIS_TEST_ON_BORROW_DEFAULT;
        String testOnBorrowStr = redisProperites.get(MonitorManagement.MONITOR_REDIS_TEST_ON_BORROW_KEY);
        if (testOnBorrowStr != null)
        {
            testOnBorrow = Boolean.parseBoolean(testOnBorrowStr);
        }
        
    }
    
    @Override
    public void start()
    {
        if (!isRunning)
        {
            isRunning = true;
            buildRedisClient();
            monitorThread = new Thread(new Runnable(){

                @Override
                public void run()
                {
                    monitorRedis();
                }
                
            });
            monitorThread.start();           
        }
        else
        {
            logger.info("redis monitor has already started");
        }
    }

    @Override
    public void stop()
    {
        if (isRunning)
        {
            isRunning = false;
            monitorThread.interrupt();
            closeRedisClient();
            try
            {
                deleteRedisRegZNode();
            } 
            catch (KeeperException e)
            {
                e.printStackTrace();
            } 
            catch (InterruptedException e)
            {
                e.printStackTrace();
            } 
            catch (ServiceUnavailableException e)
            {
                logger.info("orchestrationService is stop");
                return;
            }
        }
        else
        {
            logger.info("redis monitor has already stopped");
        }
    }
    
    private void buildRedisClient()
    {
        redisClient = new RedisClient(redisHost, redisPort, maxTotal, maxIdle, maxWait, testOnBorrow);
    }
    
    private void closeRedisClient()
    {
        if (redisClient != null)
        {
            redisClient.closePool();
            redisClient = null;
        }
    }
    
    private void monitorRedis()
    {
        logger.info("Start monitor redis");
        while(isRunning)
        {
            try
            {
                Jedis jedis = redisClient.getJedisConnection();
                redisClient.returnJedisConnection(jedis);
                try
                {
                    createRedisRegZNode();
                    Thread.sleep(REDIS_CHECK_INTERVAL);
                }
                catch (InterruptedException e)
                {
                    if(!isRunning)
                    {
                        logger.info("Close the redis worker thread");
                    }
                    else
                    {
                        e.printStackTrace();
                    }
                } 
                catch (KeeperException e)
                {
                    e.printStackTrace();
                } 
                catch (ServiceUnavailableException e)
                {
                    logger.info("orchestrationService is stop");
                    return;
                }
            }
            catch(JedisConnectionException jedisConnectionException)
            {
                closeRedisClient();
                try
                {
                    deleteRedisRegZNode();
                    Thread.sleep(MONITOR_REDIS_TIMEOUT_SLEEP);
                } 
                catch (InterruptedException e)
                {
                    if(!isRunning)
                    {
                        logger.info("Close the redis monitor worker thread");
                    }
                    else
                    {
                        e.printStackTrace();
                    }
                } 
                catch (KeeperException e)
                {
                    e.printStackTrace();
                } 
                catch (ServiceUnavailableException e)
                {
                    logger.info("orchestrationService is stop");
                    return;
                }
                buildRedisClient();
                continue;
            }
        }
    }
    
    private void createRedisRegZNode() throws KeeperException, InterruptedException, ServiceUnavailableException
    {
        if (!orchestrationService.isRegZnodeExist(redisData.getServiceName()))
        {
            orchestrationService.createRegZnode(redisData.getServiceName(), redisData);
            logger.info("Create redis znode: " + redisData.getServiceName());
        }
    }
    
    private void deleteRedisRegZNode() throws KeeperException, InterruptedException, ServiceUnavailableException
    {
        if (orchestrationService.isRegZnodeExist(redisData.getServiceName()))
        {
            orchestrationService.deleteRegZnode(redisData.getServiceName());
            logger.info("Delete redis znode: " + redisData.getServiceName());
        }
    }

}
