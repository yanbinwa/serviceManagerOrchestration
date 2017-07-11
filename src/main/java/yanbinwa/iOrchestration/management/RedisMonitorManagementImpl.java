package yanbinwa.iOrchestration.management;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import yanbinwa.common.exceptions.ServiceUnavailableException;
import yanbinwa.common.redis.RedisClient;
import yanbinwa.common.zNodedata.ZNodeServiceData;
import yanbinwa.common.zNodedata.ZNodeServiceDataImpl;
import yanbinwa.iOrchestration.service.OrchestrationService;

/**
 * 通过redisClient.getJedisConnection()方法就可以判断redis是否存活，这里有多个RedisMonitor来监控Redis服务，并创建
 * 相应的Znode
 * 
 * @author yanbinwa
 *
 */

public class RedisMonitorManagementImpl implements MonitorManagement
{
    private static final Logger logger = Logger.getLogger(RedisMonitorManagementImpl.class);
    
    private OrchestrationService orchestrationService = null;
    
    private Thread monitorThread = null;
    
    private boolean isRunning = false;
    
    private Map<String, ZNodeServiceData> redisDataMap = new HashMap<String, ZNodeServiceData>();
    private Map<String, RedisClient> redisClientMap = new HashMap<String, RedisClient>();
            
    public RedisMonitorManagementImpl(Map<String, Object> redisProperites, OrchestrationService orchestrationService)
    {
        this.orchestrationService = orchestrationService;
        
        for(Map.Entry<String, Object> entry : redisProperites.entrySet())
        {
            Object redisPropertyObj = entry.getValue();
            if (!(redisPropertyObj instanceof Map))
            {
                logger.error("Redis property should be map " + redisPropertyObj);
                continue;
            }
            @SuppressWarnings("unchecked")
            Map<String, String> redisProperty = (Map<String, String>)redisPropertyObj;
            String serviceGroupName = redisProperty.get(OrchestrationService.SERVICE_SERVICEGROUPNAME);
            if (serviceGroupName == null)
            {
                logger.error("redis service group name should not be null");
                continue;
            }
            String serviceName = redisProperty.get(OrchestrationService.SERVICE_SERVICENAME);
            if (serviceName == null)
            {
                logger.error("redis service name should not be null");
                continue;
            } 
            String redisHost = redisProperty.get(MonitorManagement.MONITOR_REDIS_HOST_KEY);
            if (redisHost == null)
            {
                logger.error("Redis host should not be null");
                continue;
            }
            String redisPortStr = redisProperty.get(MonitorManagement.MONITOR_REDIS_PORT_KEY);
            if (redisPortStr == null)
            {
                logger.error("Redis port should not be null");
                return;
            }
            int redisPort = Integer.parseInt(redisPortStr);
            ZNodeServiceData redisData = new ZNodeServiceDataImpl(redisHost, serviceGroupName, serviceName, redisPort, "redisUrl");
            redisDataMap.put(entry.getKey(), redisData);
        }
    }
    
    @Override
    public void start()
    {
        if (!isRunning)
        {
            isRunning = true;
            buildRedisClients();
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
            closeRedisClients();
            try
            {
                for (ZNodeServiceData redisData : redisDataMap.values())
                {
                    deleteRedisRegZNode(redisData);
                }
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
    
    private void buildRedisClients()
    {
        for (Map.Entry<String, ZNodeServiceData> entry : redisDataMap.entrySet())
        {
            RedisClient redisClient = buildRedisClient(entry.getValue());
            if (redisClient != null)
            {
                redisClientMap.put(entry.getKey(), redisClient);
            }
        }
    }
    
    private RedisClient buildRedisClient(ZNodeServiceData redisData)
    {
        if (redisData == null)
        {
            return null;
        }
        return new RedisClient(redisData.getIp(), redisData.getPort(), 
                MONITOR_REDIS_MAX_TOTAL_DEFAULT, MONITOR_REDIS_MAX_IDEL_DEFAULT, 
                MONITOR_REDIS_MAX_WAIT_DEFAULT, MONITOR_REDIS_TEST_ON_BORROW_DEFAULT);
    }
    
    private void closeRedisClients()
    {
        for (RedisClient redisClient : redisClientMap.values())
        {
            closeRedisClient(redisClient);
        }
        redisClientMap.clear();
    }
    
    private void closeRedisClient(RedisClient redisClient)
    {
        if (redisClient != null)
        {
            redisClient.closePool();
        }
    }
    
    private void monitorRedis()
    {
        logger.info("Start monitor redis");
        while(isRunning)
        {
            for (Map.Entry<String, ZNodeServiceData> entry : redisDataMap.entrySet())
            {
                String redisClientKey = entry.getKey();
                ZNodeServiceData redisData = entry.getValue();
                RedisClient redisClient = redisClientMap.get(redisClientKey);
                if (redisClient == null)
                {
                    redisClient = buildRedisClient(redisData);
                    redisClientMap.put(redisClientKey, redisClient);
                }
                try
                {
                    Jedis jedis = redisClient.getJedisConnection();
                    redisClient.returnJedisConnection(jedis);
                    try
                    {
                        createRedisRegZNode(redisData);
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
                    }
                }
                catch (JedisConnectionException jedisConnectionException)
                {
                    closeRedisClient(redisClient);
                    redisClientMap.remove(redisClientKey);
                    try
                    {
                        deleteRedisRegZNode(redisDataMap.get(entry.getKey()));
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
                    }
                    continue;
                }
            }
            try
            {
                Thread.sleep(REDIS_CHECK_INTERVAL);
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
        }
    }
    
    private void createRedisRegZNode(ZNodeServiceData redisData) throws KeeperException, InterruptedException, ServiceUnavailableException
    {
        if (!orchestrationService.isRegZnodeExist(redisData.getServiceName()))
        {
            orchestrationService.createRegZnode(redisData.getServiceName(), redisData);
            logger.info("Create redis znode: " + redisData.getServiceName());
        }
    }
    
    private void deleteRedisRegZNode(ZNodeServiceData redisData) throws KeeperException, InterruptedException, ServiceUnavailableException
    {
        if (orchestrationService.isRegZnodeExist(redisData.getServiceName()))
        {
            orchestrationService.deleteRegZnode(redisData.getServiceName());
            logger.info("Delete redis znode: " + redisData.getServiceName());
        }
    }
}
