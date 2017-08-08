package yanbinwa.iOrchestration.management;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.json.JSONArray;
import org.json.JSONObject;

import yanbinwa.common.exceptions.ServiceUnavailableException;
import yanbinwa.common.zNodedata.ZNodeDependenceData;
import yanbinwa.common.zNodedata.ZNodeDependenceDataImpl;
import yanbinwa.common.zNodedata.ZNodeServiceData;
import yanbinwa.common.zNodedata.decorate.ZNodeDecorateType;
import yanbinwa.iOrchestration.service.OrchestrationService;

/**
 * 
 * 如果发生改变，除了add和delete对应的serviceGroup，其它的serviceGroup信息也会更新进去，这样某个service所依赖的serviceGroup发生
 * 改变，其也能感知到。
 * 
 * @author yanbinwa
 *
 */

public class DependencyManagementImpl implements DependencyManagement
{

    private static final Logger logger = Logger.getLogger(DependencyManagementImpl.class);
    
    private Map<String, Set<String>> serviceDependenceMap = new HashMap<String, Set<String>>(); 
    
    OrchestrationService orchestrationService = null;
    
    /** 一个服务有多个实例，所以是list，key为servicgroup，value为该种servicegroup的信息，有多个实例，所以是copyOnWrite */
    Map<String, Set<String>> onLineServiceGroupToServiceNameSetMap = new HashMap<String, Set<String>>();
    
    /** key为znode的name，value为serviceData, copyOnWrite */
    Map<String, ZNodeServiceData> onLineServiceNameToServiceDataMap = new HashMap<String, ZNodeServiceData>();
    
    /** 已经上线的服务， 需要考虑线程竞争，使用copyOnWirte */
    Set<String> readyServiceGroupSet = new HashSet<String>();
    
    /** copyOnWrite lock */
    ReentrantLock lock = new ReentrantLock();
    
    KafkaTopicManagement kafkaTopicManagement = null;
    
    RedisPartitionManagement redisPartitionManagement = null;
    
    public DependencyManagementImpl(OrchestrationService orchestrationServiceImpl, JSONObject dependencyProperties)
    {
        if (orchestrationServiceImpl == null)
        {
            logger.error("orchestrationService service should not be empty");
            return;
        }
        this.orchestrationService = orchestrationServiceImpl;
        
        JSONObject serviceDependency = dependencyProperties.getJSONObject(DependencyManagement.SERVICE_DEPENDENCY_KEY);
        buildDependenceMap(serviceDependency);
        
        if (dependencyProperties.has(DependencyManagement.KAFKA_TOPIC_INFO_KEY))
        {
            JSONObject kafkaTopicInfo = dependencyProperties.getJSONObject(DependencyManagement.KAFKA_TOPIC_INFO_KEY);
            kafkaTopicManagement = new KafkaTopicManagementImpl(kafkaTopicInfo); 
        }
        
        if (dependencyProperties.has(DependencyManagement.REDIS_PARTITION_NUM_KEY))
        {
            int redisPartitionNum = dependencyProperties.getInt(DependencyManagement.REDIS_PARTITION_NUM_KEY);
            redisPartitionManagement = new RedisPartitionManagementImpl(redisPartitionNum); 
        }
    }

    @Override
    public void updateServiceDependence(List<String> currentServiceNameList) throws InterruptedException, KeeperException, ServiceUnavailableException
    {
        Map<String, ZNodeServiceData> addZNodeMap = new HashMap<String, ZNodeServiceData>();
        Map<String, ZNodeServiceData> delZNodeMap = new HashMap<String, ZNodeServiceData>();
        for(String serviceName : currentServiceNameList)
        {
            if(!onLineServiceNameToServiceDataMap.containsKey(serviceName))
            {
                ZNodeServiceData data = null;
                try
                {
                    data = orchestrationService.getRegZnodeData(serviceName);
                }
                catch(KeeperException e)
                {
                    //如果这时子节点被删除了，这里就跳过，之后会有watcher来处理的
                    if(e.code() == KeeperException.Code.NODEEXISTS)
                    {
                        logger.info("Child node has been deleted in concurrent " + e.getMessage());
                        continue;
                    }
                    else
                    {
                        logger.error("Create register or dependence node fail " + e.getMessage());
                        throw e;
                    }
                } 
                catch (ServiceUnavailableException e)
                {
                    logger.info("orchestrationService is stop");
                    return;
                }
                logger.info("Add a new service: " + serviceName + "; data is: " + data);
                addZNodeMap.put(serviceName, data);
            }
        }
        for(String serviceName : onLineServiceNameToServiceDataMap.keySet())
        {
            if(!currentServiceNameList.contains(serviceName))
            {
                logger.info("Remove a new service: " + serviceName);
                delZNodeMap.put(serviceName, onLineServiceNameToServiceDataMap.get(serviceName));
            }
        }
        if(!addZNodeMap.isEmpty() || !delZNodeMap.isEmpty())
        {
            logger.info("updateReadyService");
            updateServiceDependence(addZNodeMap, delZNodeMap);
        }
    }

    @Override
    public void reset()
    {
        onLineServiceGroupToServiceNameSetMap.clear();
        onLineServiceNameToServiceDataMap.clear();
        readyServiceGroupSet.clear();
        
        resetChildManagement();
    }

    @Override
    public JSONObject getReadyService()
    {
        logger.info("readyServiceGroupSet is: " + readyServiceGroupSet);
        logger.info("onLineServiceGroupToServiceNameSetMap is: " + onLineServiceGroupToServiceNameSetMap);
        logger.info("onLineServiceNameToServiceDataMap is: " + onLineServiceNameToServiceDataMap);
        JSONObject retObj = new JSONObject();
        for(String servicegroup : readyServiceGroupSet)
        {
            JSONObject serviceGroup = new JSONObject();
            JSONArray instanceList = new JSONArray();
            Set<String> onLineInstanceSet = onLineServiceGroupToServiceNameSetMap.get(servicegroup);
            for(String instanceName : onLineInstanceSet)
            {
                instanceList.put(onLineServiceNameToServiceDataMap.get(instanceName).createJsonObject());
            }
            serviceGroup.put("instance list", instanceList);            
            ZNodeDependenceData zNodeDependenceData = getDependencyData(servicegroup);
            serviceGroup.put("dependece list", zNodeDependenceData.createJsonObject());
            retObj.put(servicegroup, serviceGroup);
        }
        return retObj;
    }

    @Override
    public boolean isServiceReady(String serviceName)
    {
        return readyServiceGroupSet.contains(serviceName);
    }
    
    private void buildDependenceMap(JSONObject obj)
    {
        if(obj == null || serviceDependenceMap == null)
        {
            return;
        }
        for(Object keyObj : obj.keySet())
        {
            if(keyObj == null)
            {
                continue;
            }
            String key = null;
            if(keyObj instanceof String)
            {
                key = (String)keyObj;
            }
            else
            {
                logger.error("buildDependenceMap, key type should be String, " + keyObj);
                continue;
            }
            Set<String> dependenceSet = serviceDependenceMap.get(key);
            if(dependenceSet == null)
            {
                dependenceSet = new HashSet<String>();
                serviceDependenceMap.put(key, dependenceSet);
            }
            JSONArray objArr = obj.getJSONArray(key);
            if(objArr == null)
            {
                continue;
            }
            for(int i = 0; i < objArr.length(); i ++)
            {
                String serviceName = objArr.getString(i);
                dependenceSet.add(serviceName);
            }
        }
    }
    
    private void updateServiceDependence(Map<String, ZNodeServiceData> addZNodeMap, Map<String, ZNodeServiceData> delZNodeMap) throws KeeperException, InterruptedException, ServiceUnavailableException
    {
        updateChildManagement(addZNodeMap, delZNodeMap);
        Map<String, Set<String>> changedServiceGroupsMap = null;
        lock.lock();
        try
        {
            Set<String> changedGroupSet = updataOnServiceGroupAndServiceMap(addZNodeMap, delZNodeMap);
            if (changedGroupSet.size() == 0)
            {
                return;
            }
            changedServiceGroupsMap = updateReadyServiceSet();
        }
        finally
        {
            lock.unlock();
        }
        
        updateDependenceZnode(changedServiceGroupsMap);
    }
    
    private Set<String> updataOnServiceGroupAndServiceMap(Map<String, ZNodeServiceData> addZNodeMap, Map<String, ZNodeServiceData> delZNodeMap)
    {
        Map<String, Set<String>> onLineServiceGroupToServiceNameSetMapCopy = new HashMap<String, Set<String>>(onLineServiceGroupToServiceNameSetMap);
        Map<String, ZNodeServiceData> onLineServiceNameToServiceDataMapCopy = new HashMap<String, ZNodeServiceData>(onLineServiceNameToServiceDataMap);
        Set<String> changeGroupSet = new HashSet<String>();
        for(Map.Entry<String, ZNodeServiceData> entry : addZNodeMap.entrySet())
        {
            ZNodeServiceData value = entry.getValue();
            String serviceGroupName = value.getServiceGroupName();
            String serviceName = value.getServiceName();
            Set<String> serviceNameSet = onLineServiceGroupToServiceNameSetMapCopy.get(serviceGroupName);
            if(serviceNameSet == null)
            {
                serviceNameSet = new HashSet<String>();
                logger.info("ServiceGroup is on line: " + serviceGroupName);
                onLineServiceGroupToServiceNameSetMapCopy.put(serviceGroupName, serviceNameSet);
            }
            if(serviceNameSet.contains(serviceName))
            {
                logger.error("Should not contain the service: " + serviceName + "The data info is: " + value.toString());
                continue;
            }
            logger.info("Service instance is on line: " + value.toString());
            serviceNameSet.add(serviceName);
            changeGroupSet.add(serviceGroupName);
            logger.trace("serviceNameSet is: " + serviceNameSet);
        }
        for(Map.Entry<String, ZNodeServiceData> entry : delZNodeMap.entrySet())
        {
            String serviceName = entry.getKey();
            ZNodeServiceData value = entry.getValue();
            if (value == null)
            {
                logger.error("Should not contain the ZNodeServiceData: " + serviceName);
                continue;
            }
            String serviceGroupName = value.getServiceGroupName();
            if(!onLineServiceGroupToServiceNameSetMapCopy.containsKey(serviceGroupName))
            {
                logger.error("CreatedZnodeToService should contain the serviceGroup: " + serviceGroupName + "; The znode is: " + value);
                continue;
            }
            Set<String> serviceNameSet = onLineServiceGroupToServiceNameSetMapCopy.get(serviceGroupName);
            
            if(!serviceNameSet.contains(serviceName))
            {
                logger.error("Should contain the ZNodeServiceData: " + value.toString());
                continue;
            }
            logger.info("Service instance is off line: " + value.toString());
            serviceNameSet.remove(serviceName);
            changeGroupSet.add(serviceGroupName);
            if(serviceNameSet.size() == 0)
            {
                logger.info("Service group is off line: " + serviceGroupName);
                onLineServiceGroupToServiceNameSetMapCopy.remove(serviceGroupName);
            }
        }
        
        onLineServiceNameToServiceDataMapCopy.putAll(addZNodeMap);
        for(String childNode : delZNodeMap.keySet())
        {
            onLineServiceNameToServiceDataMapCopy.remove(childNode);
        }
    
        onLineServiceGroupToServiceNameSetMap = onLineServiceGroupToServiceNameSetMapCopy;
        onLineServiceNameToServiceDataMap = onLineServiceNameToServiceDataMapCopy;
        
        return changeGroupSet;
    }
    
    /**
     * 这里不仅要有添加和删除的情况，还要有更新的情况，比如一个group中新加了一个service，那么该group所对应的zNode中的数据也要更新
     * 
     * 返回需要更新的元素，包括add，change和delete
     * 
     * @throws KeeperException
     * @throws InterruptedException
     * @throws ServiceUnavailableException 
     */
    private Map<String, Set<String>> updateReadyServiceSet() throws KeeperException, InterruptedException, ServiceUnavailableException
    {
        Set<String> addReadyServiceGroup = new HashSet<String>();
        Set<String> delReadyServiceGroup = new HashSet<String>();
        //copy on write
        Set<String> readyServiceGroupSetCopy = new HashSet<String>();
        Set<String> onLineServiceGroupSet = onLineServiceGroupToServiceNameSetMap.keySet();
        for(Map.Entry<String, Set<String>> entry : serviceDependenceMap.entrySet())
        {
            boolean isReday = true;
            for(String needServiceGroup : entry.getValue())
            {
                if(!onLineServiceGroupSet.contains(needServiceGroup))
                {
                    isReday = false;
                }
            }
            //不仅要保证其依赖online，同时自己也必须online
            if (isReday && onLineServiceGroupSet.contains(entry.getKey()))
            {
                readyServiceGroupSetCopy.add(entry.getKey());
            }
        }
        
        for(String serviceGroupName : readyServiceGroupSetCopy)
        {
            if(!readyServiceGroupSet.contains(serviceGroupName))
            {
                addReadyServiceGroup.add(serviceGroupName);
                logger.trace("Add ready service group: " + serviceGroupName);
            }
        }
        
        for(String serviceGroupName : readyServiceGroupSet)
        {
            if(!readyServiceGroupSetCopy.contains(serviceGroupName))
            {
                delReadyServiceGroup.add(serviceGroupName);
                logger.trace("Delete ready service group: " + serviceGroupName);
            }
        }
        readyServiceGroupSet = readyServiceGroupSetCopy;

        // 这里处理add service group 外，均为change
        Set<String> changeReadyServiceGroup = new HashSet<String>(readyServiceGroupSet);
        for (String serviceGroup : addReadyServiceGroup)
        {
            changeReadyServiceGroup.remove(serviceGroup);
        }
        //这里的changedGroupSet应该是当前readyServiceGroupSet除掉addServiceGroup
        Map<String, Set<String>> changedServiceGroupsMap = new HashMap<String, Set<String>>();
        changedServiceGroupsMap.put(DependencyManagement.ADD_SERVICE_GROUPS_KEY, addReadyServiceGroup);
        changedServiceGroupsMap.put(DependencyManagement.DEL_SERVICE_GROUPS_KEY, delReadyServiceGroup);
        changedServiceGroupsMap.put(DependencyManagement.CHANGE_SERVICE_GROUPS_KEY, changeReadyServiceGroup);
        return changedServiceGroupsMap;
    }
    
    private void updateDependenceZnode(Map<String, Set<String>> changedServiceGroupsMap) throws KeeperException, InterruptedException, ServiceUnavailableException
    {
        //Update the znode to zookeeper
        if (changedServiceGroupsMap == null)
        {
            logger.error("changedServiceGroupsMap should not be null");
            return;
        }
        Set<String> addReadyServiceGroup = changedServiceGroupsMap.get(DependencyManagement.ADD_SERVICE_GROUPS_KEY);
        if (addReadyServiceGroup == null)
        {
            addReadyServiceGroup = new HashSet<String>();
        }
        Set<String> delReadyServiceGroup = changedServiceGroupsMap.get(DependencyManagement.DEL_SERVICE_GROUPS_KEY);
        if (delReadyServiceGroup == null)
        {
            delReadyServiceGroup = new HashSet<String>();
        }
        Set<String> changeReadyServiceGroup = changedServiceGroupsMap.get(DependencyManagement.CHANGE_SERVICE_GROUPS_KEY);
        if (changeReadyServiceGroup == null)
        {
            changeReadyServiceGroup = new HashSet<String>();
        }
        updateDependenceZnode(addReadyServiceGroup, delReadyServiceGroup, changeReadyServiceGroup);
    }
    
    /**
     * 对于addReadyServiceGroup和delReadyServiceGroup而言，其必然对应着Znode的创建和删除
     * 
     * 对于changedGroupSet而言，这里的group不一定上线，如果上线，需要update数据，如果没有，就不用处理了
     * 
     * @param addReadyServiceGroup
     * @param delReadyServiceGroup
     * @param changedGroupSet
     * @throws KeeperException
     * @throws InterruptedException
     * @throws ServiceUnavailableException 
     */
    private void updateDependenceZnode(Set<String> addReadyServiceGroup, Set<String> delReadyServiceGroup, Set<String> changedGroupSet) throws KeeperException, InterruptedException, ServiceUnavailableException
    {
        for(String serviceGroup : addReadyServiceGroup)
        {
            if (this.orchestrationService.isDepZnodeExist(serviceGroup))
            {
                logger.error("Dependence child node should not be exist: " + serviceGroup);
                continue;
            }
            ZNodeDependenceData zNodeDependenceData = getDependencyData(serviceGroup);
            this.orchestrationService.createDepZnode(serviceGroup, zNodeDependenceData);
        }
        
        for(String serviceGroup : delReadyServiceGroup)
        {
            if(!this.orchestrationService.isDepZnodeExist(serviceGroup))
            {
                logger.error("Dependence child should be exist: " + serviceGroup);
                continue;
            }
            this.orchestrationService.deleteDepZnode(serviceGroup);
        }
        
        for(String serviceGroup : changedGroupSet)
        {
            if (this.readyServiceGroupSet.contains(serviceGroup))
            {
                if(!this.orchestrationService.isDepZnodeExist(serviceGroup))
                {
                    logger.error("Dependence child should be exist: " + serviceGroup);
                    continue;
                }
                ZNodeDependenceData zNodeDependenceData = getDependencyData(serviceGroup);
                this.orchestrationService.updateDepZnode(serviceGroup, zNodeDependenceData);
            }
        }
    }
    
    /**
     * 返回两种，一种是普通的ZNodeDependenceData，一种是ZNodeDependenceDataWithKafkaTopic，需要kafkaTopicManagement来判断
     * 
     * @param serviceGroup
     * @return
     */
    private ZNodeDependenceData getDependencyData(String serviceGroup)
    {
        Map<String, Set<ZNodeServiceData>> serviceDataMap = new HashMap<String, Set<ZNodeServiceData>>();
        Set<String> dependenceServiceGroupSet = serviceDependenceMap.get(serviceGroup);
        for(String dependenceServiceGroup : dependenceServiceGroupSet)
        {
            Set<String> serviceNameSet = onLineServiceGroupToServiceNameSetMap.get(dependenceServiceGroup);
            Set<ZNodeServiceData> serviceDataSet = new HashSet<ZNodeServiceData>();
            for(String serviceName : serviceNameSet)
            {
                serviceDataSet.add(onLineServiceNameToServiceDataMap.get(serviceName));
            }
            serviceDataMap.put(dependenceServiceGroup, serviceDataSet);
        }
        /**
         * 这里默认每一个service group中kafka的producer配置是一样的，如果
         */
        ZNodeDependenceData depData = new ZNodeDependenceDataImpl(serviceDataMap);
        addZnodeDependenceDecorate(depData, serviceGroup);   
        return depData;
    }
    
    private void addZnodeDependenceDecorate(ZNodeDependenceData depData, String serviceGroup)
    {
        Set<String> serviceNameSet = onLineServiceGroupToServiceNameSetMap.get(serviceGroup);
        if (serviceNameSet == null || serviceNameSet.size() == 0)
        {
            logger.error("serviceGroup should not be null or empty " + serviceGroup);
            return;
        }
        String serviceName = new ArrayList<String>(serviceNameSet).get(0);
        ZNodeServiceData data = onLineServiceNameToServiceDataMap.get(serviceName);
        if (data == null)
        {
            logger.error("service data should not be null " + serviceName);
            return;
        }
        
        //KAFKA
        if (data.isContainedDecoreate(ZNodeDecorateType.KAFKA))
        {
            Map<String, Map<String, Set<Integer>>> topicToPartitionKeyMap = 
                    kafkaTopicManagement.getTopicGroupToTopicToPartitionKeyMappingByServiceGroup(serviceGroup);
            if (topicToPartitionKeyMap != null)
            {
                depData.addDependenceDataDecorate(ZNodeDecorateType.KAFKA, topicToPartitionKeyMap);
            }
            else
            {
                //如果只有consumer的topic info，那么就取不到topicToPartitionKeyMap  
                logger.info("Should get kafka topic partition info from service group: " + serviceGroup);
            }
        }
        //REDIS
        if (data.isContainedDecoreate(ZNodeDecorateType.REDIS))
        {
            Map<String, Set<Integer>> redisToPartitionKeyMap = redisPartitionManagement.getRedisToPartitionKeyMapping();
            if (redisToPartitionKeyMap != null)
            {
                depData.addDependenceDataDecorate(ZNodeDecorateType.REDIS, redisToPartitionKeyMap);
            }
            else
            {
                logger.error("Should get redis partition info from service group: " + serviceGroup);
            }
        }
    }
    
    private void updateChildManagement(Map<String, ZNodeServiceData> addZNodeMap, Map<String, ZNodeServiceData> delZNodeMap)
    {
        if (kafkaTopicManagement != null)
        {
            kafkaTopicManagement.updataKafkaTopicMapping(addZNodeMap, delZNodeMap);
        }
        if (redisPartitionManagement != null)
        {
            redisPartitionManagement.updataRedisPartitionMapping(addZNodeMap, delZNodeMap);
        }
    }
    
    private void resetChildManagement()
    {
        if (kafkaTopicManagement != null)
        {
            kafkaTopicManagement.reset(); 
        }
        if (redisPartitionManagement != null)
        {
            redisPartitionManagement.reset();
        }
    }
}
