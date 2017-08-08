package yanbinwa.iOrchestration.management;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import kafka.utils.ZkUtils;
import yanbinwa.common.constants.CommonConstants;
import yanbinwa.common.utils.KafkaUtil;
import yanbinwa.common.zNodedata.ZNodeServiceData;
import yanbinwa.common.zNodedata.decorate.ZNodeDecorateType;
import yanbinwa.common.zNodedata.decorate.ZNodeServiceDataDecorateKafka;

/**
 * 
 * 管理topic，这里的topic只是对应于consumer的topic，当有新的topic添加时，需要同时创建相应的topic
 * 但是旧的topic先不要删除
 * 
 * @author yanbinwa
 *
 */

public class KafkaTopicManagementImpl implements KafkaTopicManagement
{
    
    private static final Logger logger = Logger.getLogger(KafkaTopicManagementImpl.class);
    
    Map<String, Integer> topicGroupToPartitionNumMap = new HashMap<String, Integer>();
    /* consumer topic group to consumer topic */
    Map<String, Set<String>> topicGroupToTopicMap = new HashMap<String, Set<String>>();
    /* consumer topic to partitionKey */
    Map<String, Set<Integer>> topicToPartitionKeyMap = new HashMap<String, Set<Integer>>();
    /* producer service group to topic group */
    Map<String, Set<String>> serviceGroupToTopicGroupMap = new HashMap<String, Set<String>>();
    /** copyOnWrite lock */
    ReentrantLock lock = new ReentrantLock();
    
    private String zookeeperHostPort = null;
    
    public KafkaTopicManagementImpl(JSONObject kafkaTopicInfo)
    {
        zookeeperHostPort = kafkaTopicInfo.getString(ZOOKEEPER_HOST_PORT_KEY);
        if (zookeeperHostPort == null)
        {
            logger.error("zookeeperHostIp should not be null");
            return;
        }
        JSONObject topics = kafkaTopicInfo.getJSONObject(KAFKA_TOPICS_KEY);
        buildTopicGroupToPartitionNumMap(topics);
    }
    
    /**
     * 
     * 这里只是维护了所有online service的kafka topic情况，但是不保证这写service是否已经ready了，所以要在dependence service处理
     * 
     */
    @Override
    public void updataKafkaTopicMapping(Map<String, ZNodeServiceData> addZNodeMap, Map<String, ZNodeServiceData> delZNodeMap)
    {
        Map<String, Set<String>> addTopicGroupToTopicMap = new HashMap<String, Set<String>>();
        for(Map.Entry<String, ZNodeServiceData> entry : addZNodeMap.entrySet())
        {
            ZNodeServiceData zNodeServiceData = entry.getValue();
            if (zNodeServiceData == null)
            {
                continue;
            }
            updateServiceGroupToTopicGroupMap(zNodeServiceData);
            if (!(zNodeServiceData.isContainedDecoreate(ZNodeDecorateType.KAFKA)))
            {
                continue;
            }
            ZNodeServiceDataDecorateKafka decorate = (ZNodeServiceDataDecorateKafka)zNodeServiceData.getServiceDataDecorate(ZNodeDecorateType.KAFKA);
            String topicInfoStr = decorate.getTopicInfo();
            JSONObject topicInfoObj = new JSONObject(topicInfoStr);
            if (topicInfoObj.has(CommonConstants.KAFKA_CONSUMERS_KEY))
            {
                JSONObject consumersInfoObj = topicInfoObj.getJSONObject(CommonConstants.KAFKA_CONSUMERS_KEY);
                for(Object topicGroupObj : consumersInfoObj.keySet())
                {
                    if (! (topicGroupObj instanceof String))
                    {
                        logger.error("consumersInfoObj key should be String " + topicGroupObj);
                        continue;
                    }
                    String topicGroupName = (String) topicGroupObj;
                    JSONArray topicList = consumersInfoObj.getJSONArray(topicGroupName);
                    Set<String> addTopicSet = addTopicGroupToTopicMap.get(topicGroupName);
                    if (addTopicSet == null)
                    {
                        addTopicSet = new HashSet<String>();
                        addTopicGroupToTopicMap.put(topicGroupName, addTopicSet);
                    }
                    for(int i = 0; i < topicList.length(); i ++)
                    {
                        addTopicSet.add(topicList.getString(i));
                    }
                }
            }
        }
        Map<String, Set<String>> delTopicGroupToTopicMap = new HashMap<String, Set<String>>();        
        for(Map.Entry<String, ZNodeServiceData> entry : delZNodeMap.entrySet())
        {
            ZNodeServiceData zNodeServiceData = entry.getValue();
            if (zNodeServiceData == null)
            {
                continue;
            }
            updateServiceGroupToTopicGroupMap(zNodeServiceData);
            if (!(zNodeServiceData.isContainedDecoreate(ZNodeDecorateType.KAFKA)))
            {
                continue;
            }
            ZNodeServiceDataDecorateKafka decorate = (ZNodeServiceDataDecorateKafka)zNodeServiceData.getServiceDataDecorate(ZNodeDecorateType.KAFKA);
            String topicInfoStr = decorate.getTopicInfo();
            JSONObject topicInfoObj = new JSONObject(topicInfoStr);
            if (topicInfoObj.has(CommonConstants.KAFKA_CONSUMERS_KEY))
            {
                JSONObject consumersInfoObj = topicInfoObj.getJSONObject(CommonConstants.KAFKA_CONSUMERS_KEY);
                for(Object topicGroupObj : consumersInfoObj.keySet())
                {
                    if (! (topicGroupObj instanceof String))
                    {
                        logger.error("consumersInfoObj key should be String " + topicGroupObj);
                        continue;
                    }
                    String topicGroupName = (String) topicGroupObj;
                    JSONArray topicList = consumersInfoObj.getJSONArray(topicGroupName);
                    Set<String> delTopicSet = delTopicGroupToTopicMap.get(topicGroupName);
                    if (delTopicSet == null)
                    {
                        delTopicSet = new HashSet<String>();
                        delTopicGroupToTopicMap.put(topicGroupName, delTopicSet);
                    }
                    for(int i = 0; i < topicList.length(); i ++)
                    {
                        delTopicSet.add(topicList.getString(i));
                    }
                }
            }
        }  
        if (!addTopicGroupToTopicMap.isEmpty() || !delTopicGroupToTopicMap.isEmpty())
        {
            logger.info("addTopicGroupToTopicMap is: " + addTopicGroupToTopicMap + "; delTopicGroupToTopicMap is " + delTopicGroupToTopicMap);
            updateTopicToPartitionKeyMap(addTopicGroupToTopicMap, delTopicGroupToTopicMap);
        }
        if (!addTopicGroupToTopicMap.isEmpty())
        {
            logger.info("create kafka topic for addTopicGroupToTopicMap " + addTopicGroupToTopicMap);
            createAddTopic(addTopicGroupToTopicMap);
        }
    }
    
    @Override
    public Map<String, Map<String, Set<Integer>>> getTopicGroupToTopicToPartitionKeyMappingByServiceGroup(String serviceGroup)
    {
        if (serviceGroup == null)
        {
            logger.error("topicGroup set should not be null");
            return null;
        }
        Set<String> topicGroupSet = serviceGroupToTopicGroupMap.get(serviceGroup);
        if (topicGroupSet == null)
        {
            return null;
        }
        
        Map<String, Map<String, Set<Integer>>> topicGroupToTopicToPartitionKeyMapping = new HashMap<String, Map<String, Set<Integer>>>();
        for(String topicGroup : topicGroupSet)
        {
            if(topicGroupToTopicMap.containsKey(topicGroup))
            {
                Set<String> topicSet = topicGroupToTopicMap.get(topicGroup);
                if (topicSet == null || topicSet.isEmpty())
                {
                    logger.error("topicToPartitionKeyMapping should not be null or empty for topic group " + topicGroup);
                    continue;
                }
                Map<String, Set<Integer>> topicToPartitionKeyMapping = new HashMap<String, Set<Integer>>();
                for(String topic : topicSet)
                {
                    Set<Integer> partitionKeySet = topicToPartitionKeyMap.get(topic);
                    if (partitionKeySet == null || partitionKeySet.isEmpty())
                    {
                        logger.error("partitionKeySet should not be empty for topic " + topic);
                        continue;
                    }
                    topicToPartitionKeyMapping.put(topic, partitionKeySet);
                }
                topicGroupToTopicToPartitionKeyMapping.put(topicGroup, topicToPartitionKeyMapping);
            }
        }
        return topicGroupToTopicToPartitionKeyMapping;
    }

    @Override
    public void reset()
    {
        topicGroupToTopicMap.clear();
        topicToPartitionKeyMap.clear();
    }
    
    private void buildTopicGroupToPartitionNumMap(JSONObject kafkaTopicInfo)
    {
        if (kafkaTopicInfo == null)
        {
            logger.error("kafkaTopicInfo should not be null");
            return;
        }
        topicGroupToPartitionNumMap.clear();
        for(Object topicGroupObj : kafkaTopicInfo.keySet())
        {
            if (! (topicGroupObj instanceof String))
            {
                logger.error("kafkaTopicInfo key should be String " + topicGroupObj);
            }
            String topicGroup = (String)topicGroupObj;
            Integer partitionNum = kafkaTopicInfo.getInt(topicGroup);
            topicGroupToPartitionNumMap.put(topicGroup, partitionNum);
        }
    }
    
    private void updateTopicToPartitionKeyMap(Map<String, Set<String>> addTopicGroupToTopicMap, Map<String, Set<String>> delTopicGroupToTopicMap)
    {
        Set<String> changeTopicGroupSet = new HashSet<String>();
        changeTopicGroupSet.addAll(addTopicGroupToTopicMap.keySet());
        changeTopicGroupSet.addAll(delTopicGroupToTopicMap.keySet());
        lock.lock();
        try
        {
            Map<String, Set<String>> topicGroupToTopicMapCopy = new HashMap<String, Set<String>>(topicGroupToTopicMap);
            Map<String, Set<Integer>> topicToPartitionKeyMapCopy = new HashMap<String, Set<Integer>>(topicToPartitionKeyMap);
            for(String topicGroup : changeTopicGroupSet)
            {
                Set<String> addTopicSet = addTopicGroupToTopicMap.get(topicGroup);
                if (addTopicSet == null)
                {
                    addTopicSet = new HashSet<String>();
                }
                Set<String> delTopicSet = delTopicGroupToTopicMap.get(topicGroup);
                if (delTopicSet == null)
                {
                    delTopicSet = new HashSet<String>();
                }
                logger.info("topic group is " + topicGroup + "; "
                          + "addTopicSet is " + addTopicSet + "; "
                          + "topicGroupToTopicMapCopy is + " + topicGroupToTopicMapCopy + "; "
                          + "topicToPartitionKeyMapCopy is" + topicToPartitionKeyMapCopy); 
                updateTopicToPartitionKeyMap(topicGroup, addTopicSet, delTopicSet, topicGroupToTopicMapCopy, topicToPartitionKeyMapCopy);
            }
            topicGroupToTopicMap = topicGroupToTopicMapCopy;
            topicToPartitionKeyMap = topicToPartitionKeyMapCopy;
        }
        finally
        {
            lock.unlock();
        }
    }
    
    private void updateTopicToPartitionKeyMap(String topicGroupName, Set<String> addTopicSet, Set<String> delTopicSet, 
                    Map<String, Set<String>> topicGroupToTopicMapCopy, Map<String, Set<Integer>> topicToPartitionKeyMapCopy)
    {
        Set<String> curTopicSet = topicGroupToTopicMapCopy.get(topicGroupName);
        if (curTopicSet == null)
        {
            curTopicSet = new HashSet<String>();
            topicGroupToTopicMapCopy.put(topicGroupName, curTopicSet);
            logger.info("Add topic group: " + topicGroupName);
        }
        int totleTopicNum = curTopicSet.size() + addTopicSet.size() - delTopicSet.size();
        if (totleTopicNum < 0)
        {
            logger.error("Topic num for topic gourp: " + topicGroupName + " is less than 0; The cur, add and del list is "
                    + curTopicSet + "; " + addTopicSet + "; " + delTopicSet);
            topicGroupToTopicMapCopy.remove(topicGroupName);
            for (String topic : curTopicSet)
            {
                topicToPartitionKeyMapCopy.remove(topic);
            }
            return;
        }
        else if (totleTopicNum == 0)
        {
            logger.info("Remove topic group: " + topicGroupName);
            topicGroupToTopicMapCopy.remove(topicGroupName);
            for (String topic : curTopicSet)
            {
                topicToPartitionKeyMapCopy.remove(topic);
            }
            return;
        }
        
        /**
         * 先将添加的和删除的partition互换一下，其余的不动，这样之后就只剩下多出的topic或者减少的topic
         */
        int exchangeTopicNum = Math.min(addTopicSet.size(), delTopicSet.size());
        List<String> addTopicList = new ArrayList<String>(addTopicSet);
        List<String> delTopicList = new ArrayList<String>(delTopicSet);
        for(int i = 0; i < exchangeTopicNum; i ++)
        {
            String addTopic = addTopicList.get(i);
            String delTopic = delTopicList.get(i);
            Set<Integer> partitionKeySet = topicToPartitionKeyMapCopy.get(delTopic);
            if (partitionKeySet == null)
            {
                logger.info("Delete topic partitionList should not be empty");
                continue;
            }
            topicToPartitionKeyMapCopy.remove(delTopic);
            topicToPartitionKeyMapCopy.put(addTopic, partitionKeySet);
            
            addTopicSet.remove(addTopic);
            delTopicSet.remove(delTopic);
            curTopicSet.add(addTopic);
            curTopicSet.remove(delTopic);
        }
        if (addTopicSet.size() > 0)
        {
            addTopicToTopicToPartitionMap(topicGroupName, addTopicSet, topicGroupToTopicMapCopy, topicToPartitionKeyMapCopy);
        }
        else if (delTopicSet.size() > 0)
        {
            delTopicToTopicToPartitionMap(topicGroupName, delTopicSet, topicGroupToTopicMapCopy, topicToPartitionKeyMapCopy);
        }
    }
    
    private void addTopicToTopicToPartitionMap(String topicGroupName, Set<String> addTopicSet, 
                    Map<String, Set<String>> topicGroupToTopicMapCopy, Map<String, Set<Integer>> topicToPartitionKeyMapCopy)
    {
        logger.info("addTopicToTopicToPartitionMap topicGroupName: " + topicGroupName
                 + " addTopicSet: " + addTopicSet
                 + " topicGroupToTopicMapCopy: " + topicGroupToTopicMapCopy
                 + " topicToPartitionKeyMapCopy" + topicToPartitionKeyMapCopy);
        
        Set<String> curTopicList =  topicGroupToTopicMapCopy.get(topicGroupName);
        if (curTopicList == null)
        {
            logger.error("AddTopicToTopicToPartitionMap: Should never come to here");
            return;
        }
        List<Integer> avaliablePartitionKey = new ArrayList<Integer>();
        int partitionNum = getPartitionNumByTopicGroup(topicGroupName);
        int totleTopicNum = curTopicList.size() + addTopicSet.size();
        List<String> addTopicList = new ArrayList<String>(addTopicSet);
        if (totleTopicNum > partitionNum)
        {
            //TODO: 需要考虑如果之前的topic被抛弃了，但是之后又有topic down掉后，原来的topic是否可以加回来？？？
            logger.info("The topic num: " + totleTopicNum + " is larger than the partitionNum: " + partitionNum);
            if (curTopicList.size() > partitionNum)
            {
                logger.error("The current topic num should not ever larger than the partitionNum: " + topicGroupName);
                return;
            }
            int dropTopicNum = totleTopicNum - partitionNum;
            for (int i = 0; i < dropTopicNum; i ++)
            {
                String topic = addTopicList.remove(0);
                logger.info("Drop topic: " + topic + " from topic group: " + topicGroupName);
            }
        }
        int partitionNumForEachTopic = partitionNum / totleTopicNum;
        int needMigratePartitionNum = partitionNumForEachTopic * addTopicSet.size();
        logger.info("partitionNum is: " + partitionNum
                 + " totleTopicNum is " + totleTopicNum
                 + " partitionNumForEachTopic is: " + partitionNumForEachTopic
                 + " needMigratePartitionNum is: " + needMigratePartitionNum);
        
        if (curTopicList.size() == 0)
        {
            logger.info("Build topic to partitionKey for topic group: " + topicGroupName);
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
                for(String topic : curTopicList)
                {
                    Set<Integer> partitionKeySet = topicToPartitionKeyMapCopy.get(topic);
                    List<Integer> partitionKeyList = new ArrayList<Integer>(partitionKeySet);
                    if (partitionKeyList.size() > partitionNumForEachTopic)
                    {
                        int partitionKey = partitionKeyList.remove(0);
                        partitionKeySet.remove(partitionKey);
                        avaliablePartitionKey.add(partitionKey);
                        logger.info("Migrate the partitionKey: " + partitionKey + " from topic: " + topic);
                        migratePartitionNum ++;
                        if (migratePartitionNum >= needMigratePartitionNum)
                        {
                            break;
                        }
                    }
                }
            }
        }
        logger.info("avaliablePartitionKey is: " + avaliablePartitionKey);
        // 如果partition是10，但是addTopic为3，则可能就有partition漏掉的，所以这里分两步，第一步是保证addTopicSet均有
        // partitionNumForEachTopic，之后再把多余的写入到addTopic中
        for(String topic : addTopicList)
        {
            for(int i = 0; i < partitionNumForEachTopic; i ++)
            {   
                int partitionKey = avaliablePartitionKey.remove(0);
                logger.info("Migrate the partitionKey: " + partitionKey + " to topic: " + topic);
                Set<Integer> partitionKeySet = topicToPartitionKeyMapCopy.get(topic);
                if (partitionKeySet == null)
                {
                    partitionKeySet = new HashSet<Integer>();
                    topicToPartitionKeyMapCopy.put(topic, partitionKeySet);
                }
                partitionKeySet.add(partitionKey);
            }
            curTopicList.add(topic);
        }
        while(avaliablePartitionKey.size() > 0)
        {
            for (String topic : addTopicList)
            {
                int partitionKey = avaliablePartitionKey.remove(0);
                logger.info("Migrate the partitionKey: " + partitionKey + " to topic: " + topic);
                Set<Integer> partitionKeySet = topicToPartitionKeyMapCopy.get(topic);
                partitionKeySet.add(partitionKey);
                if (avaliablePartitionKey.size() <= 0)
                {
                    break;
                }
            }
        }
    }
    
    private void delTopicToTopicToPartitionMap(String topicGroupName, Set<String> delTopicSet, 
                    Map<String, Set<String>> topicGroupToTopicMapCopy, Map<String, Set<Integer>> topicToPartitionKeyMapCopy)
    {
        logger.info("addTopicToTopicToPartitionMap topicGroupName: " + topicGroupName
                + "addTopicSet: " + delTopicSet
                + "topicGroupToTopicMapCopy: " + topicGroupToTopicMapCopy
                + "topicToPartitionKeyMapCopy" + topicToPartitionKeyMapCopy);
        
        Set<String> curTopicSet = topicGroupToTopicMapCopy.get(topicGroupName);
        List<Integer> avaliablePartitionKey = new ArrayList<Integer>();
        for(String topic : delTopicSet)
        {
            Set<Integer> partitionKeySet = topicToPartitionKeyMapCopy.get(topic);
            if (partitionKeySet == null)
            {
                logger.error("Exist topic partition key list should not be null for topic " + topic);
                continue;
            }
            for (int partitionKey : partitionKeySet)
            {
                avaliablePartitionKey.add(partitionKey);
                logger.trace("Remove partitionKey: " + partitionKey + " from topic: " + topic);
            }
            topicToPartitionKeyMapCopy.remove(topic);
            curTopicSet.remove(topic);
        }
        
        if (curTopicSet.size() == 0)
        {
            logger.info("There is no topic in producer " + topicGroupName);
            topicGroupToTopicMapCopy.remove(topicGroupName);
            return;
        }
        int totleTopicNum = curTopicSet.size();
        int partitionNum = getPartitionNumByTopicGroup(topicGroupName);
        int partitionNumForEachTopic = partitionNum / totleTopicNum;
        //先保证每个topic有partitionNumForEachTopic个partition
        for(String topic : curTopicSet)
        {
            Set<Integer> partitionKeySet = topicToPartitionKeyMapCopy.get(topic);
            int leastAddNum = partitionNumForEachTopic - partitionKeySet.size();
            for(int i = 0; i < leastAddNum; i ++)
            {
                int partitionKey = avaliablePartitionKey.get(0);
                avaliablePartitionKey.remove(0);
                partitionKeySet.add(partitionKey);
                logger.info("Add partition key: " + partitionKey + " to topic: " + topic);
            }
        }
        //将多余的partitionkey再分到某些topic上
        for(String topic : curTopicSet)
        {
            if (avaliablePartitionKey.size() > 0)
            {
                int partitionKey = avaliablePartitionKey.get(0);
                avaliablePartitionKey.remove(0);
                Set<Integer> partitionKeySet = topicToPartitionKeyMapCopy.get(topic);
                partitionKeySet.add(partitionKey);
                logger.info("Add partition key: " + partitionKey + " to topic: " + topic);
            }
        }
    }
    
    private int getPartitionNumByTopicGroup(String topicGroup)
    {
        Integer partitionNum = topicGroupToPartitionNumMap.get(topicGroup);
        if (partitionNum == null)
        {
            partitionNum = CommonConstants.KAFKA_DEFAULT_PARTITION_NUM;
        }
        return partitionNum;
    }
    
    private void updateServiceGroupToTopicGroupMap(ZNodeServiceData zNodeServiceData)
    {
        if (zNodeServiceData == null)
        {
            return;
        }
        lock.lock();
        try
        {
            Set<String> topicGroupSet = getTopicGroupSetByZNodeServiceData(zNodeServiceData);
            String serviceGroupName = zNodeServiceData.getServiceGroupName();
            if (topicGroupSet == null)
            {
                serviceGroupToTopicGroupMap.remove(serviceGroupName);
            }
            else
            {
                serviceGroupToTopicGroupMap.put(serviceGroupName, topicGroupSet);
            }
        }
        finally
        {
            lock.unlock();
        }
    }
    
    private Set<String> getTopicGroupSetByZNodeServiceData(ZNodeServiceData zNodeServiceData)
    {
        if (zNodeServiceData == null || !(zNodeServiceData.isContainedDecoreate(ZNodeDecorateType.KAFKA)))
        {
            return null;
        }
        ZNodeServiceDataDecorateKafka decorate = (ZNodeServiceDataDecorateKafka)zNodeServiceData.getServiceDataDecorate(ZNodeDecorateType.KAFKA);
        String topicInfoStr = decorate.getTopicInfo();
        JSONObject topicInfoObj = new JSONObject(topicInfoStr);
        if (!topicInfoObj.has(CommonConstants.KAFKA_PRODUCERS_KEY))
        {
            return null;
        }
        Set<String> topicGroupSet = new HashSet<String>();
        JSONArray topicGroupSetObj = topicInfoObj.getJSONArray(CommonConstants.KAFKA_PRODUCERS_KEY);
        for(int i = 0; i < topicGroupSetObj.length(); i ++)
        {
            topicGroupSet.add(topicGroupSetObj.getString(i));
        }
        return topicGroupSet;
    }
    
    private void createAddTopic(Map<String, Set<String>> addTopicGroupToTopicMap)
    {
        if (addTopicGroupToTopicMap == null || addTopicGroupToTopicMap.isEmpty())
        {
            return;
        }
        Map<String, Object> zookeeperProperties = new HashMap<String, Object>();
        zookeeperProperties.put(CommonConstants.ZOOKEEPER_HOSTPORT_KEY, zookeeperHostPort);
        ZkUtils zkUtils = KafkaUtil.createZkUtils(zookeeperProperties);
        try
        {
            Map<String, Object> topicProperties = new HashMap<String, Object>();
            for(Set<String> topicSet : addTopicGroupToTopicMap.values())
            {
                if (topicSet == null || topicSet.isEmpty())
                {
                    continue;
                }
                for (String topic : topicSet)
                {
                    if (topic == null)
                    {
                        continue;
                    }
                    topicProperties.put(CommonConstants.KAFKA_TOPIC_KEY, topic);
                    if (!KafkaUtil.isTopicExist(zkUtils, topicProperties))
                    {
                        logger.info("Create topic " + topic);
                        KafkaUtil.createTopic(zkUtils, topicProperties);
                    }
                    else
                    {
                        logger.info("topic " + topic + " is existed. Does not need to create");
                    }
                }
            }
        }
        finally
        {
            KafkaUtil.closeZkUtils(zkUtils);
        }
    }

}
