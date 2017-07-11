package yanbinwa.iOrchestration.management;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import yanbinwa.common.zNodedata.ZNodeServiceData;

public class RedisPartitionManagementImpl implements RedisPartitionManagement
{

    private static final Logger logger = Logger.getLogger(RedisPartitionManagementImpl.class);
    
    Map<String, Set<Integer>> redisToPartitionMap = new HashMap<String, Set<Integer>>();
    ReentrantLock lock = new ReentrantLock();
    
    int partitionNum = -1;
    
    public RedisPartitionManagementImpl(int partitionNum)
    {
        if (partitionNum < 1)
        {
            logger.error("The redis partitionNum should not less then 1");
            partitionNum = REDIS_PARTITIONNUM_DEFAULT;
        }
        this.partitionNum = partitionNum;
    }
    
    @Override
    public void updataRedisPartitionMapping(Map<String, ZNodeServiceData> addZNodeMap, Map<String, ZNodeServiceData> delZNodeMap)
    {
        List<String> addRedisServiceName = new ArrayList<String>(); 
        for (ZNodeServiceData data : addZNodeMap.values())
        {
            if (data == null)
            {
                continue;
            }
            if (! data.getServiceGroupName().equals(REDIS_SERVICEGROUP_KEY))
            {
                continue;
            }
            addRedisServiceName.add(data.getServiceName());
        }
        
        List<String> delRedisServiceName = new ArrayList<String>(); 
        for (ZNodeServiceData data : delZNodeMap.values())
        {
            if (data == null)
            {
                continue;
            }
            if (! data.getServiceGroupName().equals(REDIS_SERVICEGROUP_KEY))
            {
                continue;
            }
            delRedisServiceName.add(data.getServiceName());
        }
        
        if (!addRedisServiceName.isEmpty() || !delRedisServiceName.isEmpty())
        {
            logger.info("addRedisServiceName is: " + addRedisServiceName + "; delRedisServiceName is " + delRedisServiceName);
            updateRedisToPartitionKeyMap(addRedisServiceName, delRedisServiceName);
        }
    }

    @Override
    public Map<String, Set<Integer>> getRedisToPartitionKeyMapping()
    {
        return redisToPartitionMap;
    }

    @Override
    public void reset()
    {
        redisToPartitionMap.clear();
    }
    
    private void updateRedisToPartitionKeyMap(List<String> addRedisServiceName, List<String> delRedisServiceName)
    {
        lock.lock();
        try
        {
            Map<String, Set<Integer>> redisToPartitionMapTmp = new HashMap<String, Set<Integer>>(redisToPartitionMap);
            List<String> currentRedisServiceName = new ArrayList<String> (redisToPartitionMapTmp.keySet());
            int totalRedisService = currentRedisServiceName.size() + addRedisServiceName.size() - delRedisServiceName.size();
            if (totalRedisService < 0)
            {
                logger.error("Redis num is less than 0; The cur, add and del list is "
                        + currentRedisServiceName + "; " + addRedisServiceName + "; " + delRedisServiceName);
                redisToPartitionMap.clear();
                return;
            }
            int exchangeRedisNum = Math.min(addRedisServiceName.size(), delRedisServiceName.size());
            int i = 0;
            for (; i < exchangeRedisNum; i ++)
            {
                Set<Integer> partitionKeys = redisToPartitionMapTmp.remove(delRedisServiceName.get(i));
                redisToPartitionMapTmp.put(addRedisServiceName.get(i), partitionKeys);
            }
            if (addRedisServiceName.size() > i)
            {
                addRedisToPartitionMap(redisToPartitionMapTmp, addRedisServiceName.subList(i, addRedisServiceName.size()));
            }
            else if (delRedisServiceName.size() > i)
            {
                deleteRedisToPartitionMap(redisToPartitionMapTmp, delRedisServiceName.subList(i, delRedisServiceName.size()));
            }
            redisToPartitionMap = redisToPartitionMapTmp;
        }
        finally
        {
            lock.unlock();
        }
    }
    
    private void addRedisToPartitionMap(Map<String, Set<Integer>> redisToPartitionMapTmp, List<String> addRedisServiceName)
    {
        List<String> currentRedisServiceName = new ArrayList<String> (redisToPartitionMapTmp.keySet());
        int totalRedisNum = currentRedisServiceName.size() + addRedisServiceName.size();
        // 如果redis数量已经大于partition数量，多余的redis也不在起作用，这里就直接不分配了
        if (totalRedisNum > partitionNum)
        {
            logger.info("The totel redis num: " + totalRedisNum + " is larger than the partitionNum: " + partitionNum);
            if (currentRedisServiceName.size() > partitionNum)
            {
                logger.error("The current redis num is larger than the partitionNum, This should not ever happen");
                return;
            }
            int dropRedisNum = totalRedisNum - partitionNum;
            for (int i = 0; i < dropRedisNum; i ++)
            {
                String redisServiceName = addRedisServiceName.remove(0);
                logger.info("Drop redis service: " + redisServiceName);
            }
        }
        int partitionNumForEachRedis = partitionNum / totalRedisNum;
        int needMigratePartitionNum = partitionNumForEachRedis * addRedisServiceName.size();
        List<Integer> avaliablePartitionKey = new ArrayList<Integer>();
        logger.info("partitionNum is: " + partitionNum
                + " totleTopicNum is " + totalRedisNum);
        if (currentRedisServiceName.size() == 0)
        {
            logger.info("Build redis to partitionKey");
            for (int i = 0; i < partitionNum; i ++)
            {
                avaliablePartitionKey.add(i);
            }
        }
        else
        {
            int migratePartitionNum = 0;
            while(migratePartitionNum < needMigratePartitionNum)
            {
                for(String redisServiceName : currentRedisServiceName)
                {
                    Set<Integer> partitionKeySet = redisToPartitionMapTmp.get(redisServiceName);
                    List<Integer> partitionKeyList = new ArrayList<Integer>(partitionKeySet);
                    if (partitionKeyList.size() > partitionNumForEachRedis)
                    {
                        int partitionKey = partitionKeyList.remove(0);
                        partitionKeySet.remove(partitionKey);
                        avaliablePartitionKey.add(partitionKey);
                        migratePartitionNum ++;
                        logger.info("Migrate the partitionKey: " + partitionKey + " from redis: " + redisServiceName);
                        if (migratePartitionNum >= needMigratePartitionNum)
                        {
                            break;
                        }
                    }
                }
            }
        }
        logger.info("avaliablePartitionKey is: " + avaliablePartitionKey);
        for(String redisServiceName : addRedisServiceName)
        {
            for(int i = 0; i < partitionNumForEachRedis; i ++)
            {   
                int partitionKey = avaliablePartitionKey.remove(0);
                logger.info("Migrate the partitionKey: " + partitionKey + " to redis: " + redisServiceName);
                Set<Integer> partitionKeySet = redisToPartitionMapTmp.get(redisServiceName);
                if (partitionKeySet == null)
                {
                    partitionKeySet = new HashSet<Integer>();
                    redisToPartitionMapTmp.put(redisServiceName, partitionKeySet);
                }
                partitionKeySet.add(partitionKey);
            }
        }
        while(avaliablePartitionKey.size() > 0)
        {
            for (String redisServiceName : addRedisServiceName)
            {
                int partitionKey = avaliablePartitionKey.remove(0);
                logger.info("Migrate the partitionKey: " + partitionKey + " to redis: " + redisServiceName);
                Set<Integer> partitionKeySet = redisToPartitionMapTmp.get(redisServiceName);
                partitionKeySet.add(partitionKey);
                if (avaliablePartitionKey.size() == 0)
                {
                    break;
                }
            }
        }
    }
    
    private void deleteRedisToPartitionMap(Map<String, Set<Integer>> redisToPartitionMapTmp, List<String> delRedisServiceName)
    {
        List<String> currentRedisServiceName = new ArrayList<String> (redisToPartitionMapTmp.keySet());
        int totalRedisNum = currentRedisServiceName.size() - delRedisServiceName.size();
        if (totalRedisNum == 0)
        {
            logger.info("All redis service has been deleted");
            redisToPartitionMapTmp.clear();
            return;
        }
        int partitionNumForEachRedis = partitionNum / totalRedisNum;
        List<Integer> avaliablePartitionKey = new ArrayList<Integer>();
        for (String deleteRedis : delRedisServiceName)
        {
            Set<Integer> partitionKeys = redisToPartitionMapTmp.remove(deleteRedis);
            logger.info("delete redis service: " + deleteRedis + "; Release the partitionKeys: " + partitionKeys);
            avaliablePartitionKey.addAll(partitionKeys);
        }
        currentRedisServiceName = new ArrayList<String> (redisToPartitionMapTmp.keySet());
        for (String curRedisService : currentRedisServiceName)
        {
            Set<Integer> partitionKeys = redisToPartitionMapTmp.get(curRedisService);
            if (partitionKeys == null)
            {
                logger.error("partitionKeys for redis service: " + curRedisService + " should not be empty");
                partitionKeys = new HashSet<Integer>();
                redisToPartitionMapTmp.put(curRedisService, partitionKeys);
            }
            while(partitionKeys.size() < partitionNumForEachRedis)
            {
                int partitionkey = avaliablePartitionKey.remove(0);
                logger.info("add partition key: " + partitionkey + " to redis: " + curRedisService);
                partitionKeys.add(partitionkey);
            }
        }
        while(avaliablePartitionKey.size() > 0)
        {
            for(String curRedisService : currentRedisServiceName)
            {
                Set<Integer> partitionKeys = redisToPartitionMapTmp.get(curRedisService);
                int partitionkey = avaliablePartitionKey.remove(0);
                logger.info("add partition key: " + partitionkey + " to redis: " + curRedisService);
                partitionKeys.add(partitionkey);
                if (avaliablePartitionKey.size() == 0)
                {
                    break;
                }
            }
        }
    }

}
