package yanbinwa.iOrchestration.service;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Service;

import yanbinwa.common.constants.CommonConstants;
import yanbinwa.common.utils.ZkUtil;
import yanbinwa.common.zNodedata.ZNodeData;
import yanbinwa.common.zNodedata.ZNodeDataUtil;
import yanbinwa.common.zNodedata.ZNodeDependenceData;
import yanbinwa.common.zNodedata.ZNodeServiceData;
import yanbinwa.iOrchestration.exception.ServiceUnavailableException;

/**
 * 
 * orchestration还要监听kafka的状态，因为kafka也是一个重要的依赖
 * 
 * orchestration的依赖是通过servicegroup来确定的，是servicegroup之前的依赖，而不是特定service之间的依赖
 * 这里要做到所有servicegroup都可以伸缩
 * 
 * @author yanbinwa
 *
 */

@Service("orchestrationService")
@EnableAutoConfiguration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "orchestration")
public class OrchestrationServiceImpl implements OrchestrationService
{

    private static final Logger logger = Logger.getLogger(OrchestrationServiceImpl.class);
    
    @Value("${orchestration.dependences:}")
    String dependences = null;
    
    ZNodeServiceData serviceData = null;
    
    String regZnodePath = null;
    String regZnodeChildPath = null;
    String depZnodePath = null;
    String zookeeperHostport = null;
    
    Map<String, String> serviceDataProperties;
    Map<String, String> zNodeInfoProperties;
    Map<String, String> kafkaProperties;
    
    public void setServiceDataProperties(Map<String, String> properties)
    {
        this.serviceDataProperties = properties;
    }
    
    public Map<String, String> getServiceDataProperties()
    {
        return this.serviceDataProperties;
    }
    
    public void setZNodeInfoProperties(Map<String, String> properties)
    {
        this.zNodeInfoProperties = properties;
    }
    
    public Map<String, String> getZNodeInfoProperties()
    {
        return this.zNodeInfoProperties;
    }
    
    public void setKafkaProperties(Map<String, String> properties)
    {
        this.kafkaProperties = properties;
    }
    
    public Map<String, String> getKafkaProperties()
    {
        return this.kafkaProperties;
    }
    
    /** 不用考虑线程竞争，因为其只在初始化时修改，其它时间是查询，这里是servicegroup的依赖关系 */
    Map<String, Set<String>> dependenceMap = new HashMap<String, Set<String>>(); 
    
    /** 一个服务有多个实例，所以是list，key为servicgroup，value为该种servicegroup的信息，有多个实例，所以是copyOnWrite */
    Map<String, Set<String>> onLineServiceGroupToServiceNameSetMap = new HashMap<String, Set<String>>();
    
    /** key为znode的name，value为serviceData, copyOnWrite */
    Map<String, ZNodeData> onLineServiceNameToServiceDataMap = new HashMap<String, ZNodeData>();
    
    /** 已经上线的服务， 需要考虑线程竞争，使用copyOnWirte */
    Set<String> readyServiceGroupSet = new HashSet<String>();
    
    /** copyOnWrite lock */
    ReentrantLock lock = new ReentrantLock();
    
    /** 主要处理zookeeper事件的方法*/
    Thread zookeeperThread = null;
    
    Thread zookeeperSync = null;
    
    /** standby, active or stop */
    AtomicInteger statue = new AtomicInteger(CommonConstants.SERVICE_STANDBY);
    
    /** 存放监听到的Zookeeper信息 */
    BlockingQueue<WatchedEvent> zookeeperEventQueue = new LinkedBlockingQueue<WatchedEvent>();
    
    /** Zookeeper connection */
    ZooKeeper zk = null;
    
    boolean isRunning = false;
    
    Watcher zkWatcher = new ZkWatcher();
    
    KafkaMonitor kafkaMonitor = null;
    
    public Map<String, Set<String>> getDependenceMap()
    {
        return this.dependenceMap;
    }
    
    @Override
    public void afterPropertiesSet() throws Exception
    {
        dependenceMap = new HashMap<String, Set<String>>();
        if(dependences == null)
        {
            return;
        }
        init();
        start();
    }

    private void init()
    {
        /** 读取dependence信息 */
        JSONObject dependenceObj = new JSONObject(dependences);
        buildDependenceMap(dependenceObj, dependenceMap);
        
        String serviceGroup = serviceDataProperties.get(OrchestrationService.SERVICE_SERVICEGROUPNAME);
        String serviceName = serviceDataProperties.get(OrchestrationService.SERVICE_SERVICENAME);
        String ip = serviceDataProperties.get(OrchestrationService.SERVICE_IP);
        String portStr = serviceDataProperties.get(OrchestrationService.SERVICE_PORT);
        int port = Integer.parseInt(portStr);
        String rootUrl = serviceDataProperties.get(OrchestrationService.SERVICE_ROOTURL);
        serviceData = new ZNodeServiceData(ip, serviceGroup, serviceName, port, rootUrl);
        
        regZnodePath = zNodeInfoProperties.get(OrchestrationService.ZNODE_REGPATH);
        regZnodeChildPath = zNodeInfoProperties.get(OrchestrationService.ZNODE_REGCHILDPATH);
        depZnodePath = zNodeInfoProperties.get(OrchestrationService.ZNODE_DEPPATH);
        zookeeperHostport = zNodeInfoProperties.get(OrchestrationService.ZK_HOSTPORT);
        
        if (kafkaProperties != null)
        {
            kafkaMonitor = new KafkaMonitor(kafkaProperties);
        }
    }
    
    @Override
    public JSONObject getReadyService() throws ServiceUnavailableException
    {
        if (!isRunning)
        {
            throw new ServiceUnavailableException();
        }
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
    public void start()
    {
        if(!isRunning)
        {
            logger.info("Start orchestration serivce ...");
            isRunning = true;
            /** 连接Zookeeper，创建相应的Znode，并监听其它服务创建的Znode */
            zookeeperThread = new Thread(new Runnable() {

                @Override
                public void run()
                {
                    zookeeperEventHandler();
                }
                
            });
            zookeeperThread.start();
            
            /** 定期创建一个WatcherEvent, 让服务自动与zookeeper同步 */
            zookeeperSync = new Thread(new Runnable() {

                @Override
                public void run()
                {
                    syncWithZookeeper();
                }
                
            });
            zookeeperSync.start();
        }
        else
        {
            logger.info("Orchestration serivce has readly started ...");
        }
    }
    
    @Override
    public void stop()
    {
        if (isRunning)
        {
            logger.info("Stop orchestration serivce ...");
            isRunning = false;
            //暂停kafka monitor
            if (kafkaMonitor != null)
            {
                kafkaMonitor.stop();
            }
            if (zookeeperThread != null)
            {
                zookeeperThread.interrupt();
                zookeeperThread = null;
            }
            if (zookeeperSync != null)
            {
                zookeeperSync.interrupt();
                zookeeperSync = null;
            }
            statue.set(CommonConstants.SERVICE_STANDBY);
            onLineServiceGroupToServiceNameSetMap = new HashMap<String, Set<String>>();
            onLineServiceNameToServiceDataMap = new HashMap<String, ZNodeData>();
            readyServiceGroupSet = new HashSet<String>();
        }
        else
        {
            logger.info("Orchestration serivce has ready stopped...");
        }
    }
    
    /**
     * 因为只需要监测register node的情况，所以只要与register node进行同步
     * 
     */
    private void syncWithZookeeper()
    {
        while(isRunning && zk != null)
        {
            WatchedEvent event = new WatchedEvent(Watcher.Event.EventType.NodeChildrenChanged, null, regZnodePath);
            zookeeperEventQueue.offer(event);
            try
            {
                Thread.sleep(OrchestrationService.ZK_SYNC_INTERVAL);
            } 
            catch (InterruptedException e)
            {
                if(!isRunning)
                {
                    logger.info("Stopping the orchestration service");
                }
                else
                {
                    e.printStackTrace();
                }
            }
        }
    }
    
    private void buildDependenceMap(JSONObject obj, Map<String, Set<String>> iDependenceMap)
    {
        if(obj == null || iDependenceMap == null)
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
            Set<String> dependenceSet = iDependenceMap.get(key);
            if(dependenceSet == null)
            {
                dependenceSet = new HashSet<String>();
                iDependenceMap.put(key, dependenceSet);
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
    
    class ZkWatcher implements Watcher
    {
        @Override
        public void process(WatchedEvent event)
        {
            zookeeperEventQueue.offer(event);
        } 
    }
    
    private void setUpZnodeForActive() throws KeeperException, InterruptedException
    {
        logger.info("setUpZnodeForActive ...");
        
        if (ZkUtil.checkZnodeExist(zk, regZnodePath))
        {
            ZkUtil.setData(zk, regZnodePath, serviceData.createJsonObject());
        }
        else
        {
            String regZNodePathStr = ZkUtil.createPersistentZNode(zk, regZnodePath, serviceData.createJsonObject());
            logger.info("Create znode: " + regZNodePathStr);
        }
        if (ZkUtil.checkZnodeExist(zk, depZnodePath))
        {
            ZkUtil.setData(zk, depZnodePath, serviceData.createJsonObject());
        }
        else
        {
            String depZNodePathStr = ZkUtil.createPersistentZNode(zk, depZnodePath, serviceData.createJsonObject());
            logger.info("Create znode: " + depZNodePathStr);
        }
        String regZNodeChildPathStr = ZkUtil.createEphemeralZNode(zk, regZnodeChildPath, serviceData.createJsonObject());
        logger.info("Create znode: " + regZNodeChildPathStr);
    }
    
    private void waitingForZookeeper()
    {
        logger.info("Waiting for the zookeeper...");
        while(zk.getState() == ZooKeeper.States.CONNECTING && isRunning)
        {
            try
            {
                Thread.sleep(ZK_WAIT_INTERVAL);
                logger.debug("Try to connection to zookeeper");
            } 
            catch (InterruptedException e)
            {
                if (!isRunning)
                {
                    logger.info("Stop this thread");
                }
                else
                {
                    e.printStackTrace();
                }
            }            
        }
        logger.info("Connected to the zookeeper " + zookeeperHostport);
    }
    
    private void zookeeperEventHandler()
    {
        isRunning = true;
        if(zk != null)
        {
            try
            {
                ZkUtil.closeZk(zk);
                zk = null;
            } 
            catch (InterruptedException e)
            {
                logger.error("Fail to close the zookeeper connection at begin");
            }
        }
        zk = ZkUtil.connectToZk(zookeeperHostport, zkWatcher);
        if (zk == null)
        {
            logger.error("Can not connect to zookeeper: " + zookeeperHostport);
            return;
        }
        if(zk.getState() == ZooKeeper.States.CONNECTING)
        {
            waitingForZookeeper();
        }
        try
        {
            //查看regZnodePath是否存在
            if (!ZkUtil.checkZnodeExist(zk, regZnodePath))
            {
                setUpZnodeForActive();
                statue.set(CommonConstants.SERVICE_ACTIVE);
                
            }
            else if(!ZkUtil.checkZnodeExist(zk, regZnodeChildPath))
            {
                //这里有两种情况，一种是有其它的服务已经创建了父目录，但还没来得及创建子目录，另一种情况是之前服务异常退出，没有来得及删除
                Thread.sleep(OrchestrationService.ZKNODE_REGCHILDPATH_WAITTIME);
                //等待建立子目录，但是还没有建立说明是之前异常退出时遗留的
                if(!ZkUtil.checkZnodeExist(zk, regZnodeChildPath))
                {
                    setUpZnodeForActive();
                    statue.set(CommonConstants.SERVICE_ACTIVE);
                }
            }
            ZkUtil.watchZnodeChildeChange(zk, regZnodePath, zkWatcher);
            while(isRunning)
            {
                if (statue.get() == CommonConstants.SERVICE_STANDBY)
                {
                    handerZookeeperEventAsStandby();
                }
                else
                {
                    handerZookeeperEventAsActive();
                }
            }
        } 
        catch (KeeperException e)
        {
            logger.error(e.getMessage());
        } 
        catch (InterruptedException e)
        {
            if(!isRunning)
            {
                logger.info("Close this thread...");
            }
            else
            {
                e.printStackTrace();
            }
        }
        finally
        {
            try
            {
                if (zk != null)
                {
                    ZkUtil.closeZk(zk);
                }
            } 
            catch (InterruptedException e)
            {
                logger.error("Fail to close the zookeeper connection");
            }
        }
    }
    
    private void handerZookeeperEventAsStandby()
    {
        logger.info("Starting handler zookeepr event for standby... ");
        try
        {
            ZkUtil.watchZnodeChildeChange(zk, regZnodePath, zkWatcher);
            while(isRunning && statue.get() == CommonConstants.SERVICE_STANDBY)
            {
                WatchedEvent event = zookeeperEventQueue.poll(OrchestrationService.ZKEVENT_QUEUE_TIMEOUT, 
                                                                                            TimeUnit.MILLISECONDS);
                if(event == null)
                {
                    continue;
                }
                logger.debug("Get zk event at standby mode: " + event.toString());
                if(event.getType() == Watcher.Event.EventType.NodeChildrenChanged)
                {
                    //这里观察到regZnodeChildPath消失了，说明主Register down掉了
                    if (!ZkUtil.checkZnodeExist(zk, regZnodeChildPath))
                    {
                        statue.set(CommonConstants.SERVICE_ACTIVE);
                        return;
                    }
                }
                else if (event.getType() == Watcher.Event.EventType.None && event.getState() == Watcher.Event.KeeperState.SyncConnected)
                {
                    logger.info("Connected to zookeeper success!");
                }
                else
                {
                    logger.error("Unknown event: " + event.toString());
                }
                ZkUtil.watchZnodeChildeChange(zk, regZnodePath, zkWatcher);
            } 
        }
        catch (KeeperException e)
        {
            logger.error("handerZookeeperEventAsStandby meet error " + e.getMessage());
        }
        catch (InterruptedException e)
        {
            if(!isRunning)
            {
                logger.info("Close this thread...");
            }
            else
            {
                e.printStackTrace();
            }
        }
        logger.info("End handler zookeepr event for standby... ");
    }
    
    private void handerZookeeperEventAsActive()
    {
        logger.info("Starting handler zookeepr event for active... ");
        try
        {
            if (!ZkUtil.checkZnodeExist(zk, regZnodeChildPath))
            {
                //说明是由standy转换过来的，需要创建临时节点，并且更新watcher
                String regZNodeChildPathStr = ZkUtil.createEphemeralZNode(zk, regZnodeChildPath, serviceData.createJsonObject());
                logger.info("Create child znode: " + regZNodeChildPathStr);
                ZkUtil.watchZnodeChildeChange(zk, regZnodePath, zkWatcher);
                //更新ActiveService信息
                ZkUtil.setData(zk, regZnodePath, serviceData.createJsonObject());
                ZkUtil.setData(zk, depZnodePath, serviceData.createJsonObject());
                //提前先做一次扫描
                handerChildNodeChange(this.regZnodePath);
            }
            else
            {
                ZNodeServiceData data = new ZNodeServiceData(ZkUtil.getData(zk, regZnodePath));
                //被其它的device抢先设置了
                if(!data.equals(this.serviceData))
                {
                    statue.set(CommonConstants.SERVICE_STANDBY);
                    return;
                }
            }
            //开始kafkaMonitor
            if (kafkaMonitor != null)
            {
                kafkaMonitor.start();
            }
            
            while(isRunning && statue.get() == CommonConstants.SERVICE_ACTIVE)
            {
                WatchedEvent event = zookeeperEventQueue.poll(OrchestrationService.ZKEVENT_QUEUE_TIMEOUT, 
                                                                                            TimeUnit.MILLISECONDS);
                if(event == null)
                {
                    continue;
                }
                logger.debug("Get zk event at active mode: " + event.toString());
                if(event.getType() == Watcher.Event.EventType.NodeChildrenChanged)
                {
                    handerChildNodeChange(event.getPath());
                }
                else if (event.getType() == Watcher.Event.EventType.None && event.getState() == Watcher.Event.KeeperState.SyncConnected)
                {
                    logger.info("Connected to zookeeper success!");
                }
                else
                {
                    logger.error("Unknown event: " + event.toString());
                }
                ZkUtil.watchZnodeChildeChange(zk, regZnodePath, zkWatcher);
            }
        }
        catch (KeeperException e)
        {
            if(e instanceof KeeperException.SessionExpiredException)
            {
                logger.info("zookeepr session is expired, need to reconnect");
                if (zk != null)
                {
                    try
                    {
                        ZkUtil.closeZk(zk);
                    } 
                    catch (InterruptedException e1)
                    {
                        //do nothing
                    }
                }
                statue.set(CommonConstants.SERVICE_STANDBY);
                onLineServiceGroupToServiceNameSetMap.clear();
                onLineServiceNameToServiceDataMap.clear();
                readyServiceGroupSet.clear();
                zk = ZkUtil.connectToZk(zookeeperHostport, zkWatcher);
                if (zk == null)
                {
                    logger.error("Can not connect to zookeeper: " + zookeeperHostport);
                    return;
                }
                if(zk.getState() == ZooKeeper.States.CONNECTING)
                {
                    waitingForZookeeper();
                }
            }
            else
            {
                e.printStackTrace();
            }
        }
        catch (InterruptedException e)
        {
            if(!isRunning)
            {
                logger.info("Close this thread...");
            }
            else
            {
                e.printStackTrace();
            }
        }
        logger.info("End handler zookeepr event for active... ");
    }
    
    private void handerChildNodeChange(String parentPath) throws KeeperException, InterruptedException
    {
        String path = parentPath;
        //获取到的是相对路径，或znode的名称，例如node10000000000
        List<String> childList = ZkUtil.getChildren(zk, path);
        logger.debug("Get current children list: " + childList);
        
        Map<String, ZNodeData> addZNodeMap = new HashMap<String, ZNodeData>();
        Set<String> delZNodeSet = new HashSet<String>();
        for(String childNode : childList)
        {
            //childNode即为serviceName，排除register子node
            String childPath = getRegZnodePathForChildNode(childNode);
            if(!onLineServiceNameToServiceDataMap.containsKey(childNode) && !childPath.equals(regZnodeChildPath))
            {
                JSONObject data = null;
                try
                {
                    data = ZkUtil.getData(zk, childPath);
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
                logger.info("Add a new childnode: " + childNode + "; data is: " + data);
                ZNodeData zNodeData = ZNodeDataUtil.getZnodeData(data);
                addZNodeMap.put(childNode, zNodeData);
            }
        }
        for(String childNode : onLineServiceNameToServiceDataMap.keySet())
        {
            if(!childList.contains(childNode))
            {
                logger.info("Remove a new childnode: " + childNode);
                delZNodeSet.add(childNode);
            }
        }
        if(!addZNodeMap.isEmpty() || !delZNodeSet.isEmpty())
        {
            logger.info("updateReadyService");
            updateReadyService(addZNodeMap, delZNodeSet);
        }
    }
    
    //会计算出readyService的副本，根据副本与原来的差异来创建或者删除dependence znode
    private void updateReadyService(Map<String, ZNodeData> addZNodeMap, Set<String> delZNodeSet) throws KeeperException, InterruptedException
    {
        copyOnWriteForMap(addZNodeMap, delZNodeSet);
        updateReadyService();
    }
    
    private void copyOnWriteForMap(Map<String, ZNodeData> addZNodeMap, Set<String> delZNodeSet)
    {
        //copy on write
        lock.lock();
        try
        {
            Map<String, Set<String>> onLineServiceGroupToServiceNameSetMapCopy = new HashMap<String, Set<String>>(onLineServiceGroupToServiceNameSetMap);
            for(Map.Entry<String, ZNodeData> entry : addZNodeMap.entrySet())
            {
                ZNodeData value = entry.getValue();
                String serviceGroupName = value.getServiceGroupName();
                String serviceName = value.getServiceName();
                Set<String> serviceNameSet = onLineServiceGroupToServiceNameSetMap.get(serviceGroupName);
                if(serviceNameSet == null)
                {
                    serviceNameSet = new HashSet<String>();
                    logger.info("ServiceGroup is on line: " + serviceGroupName);
                    onLineServiceGroupToServiceNameSetMapCopy.put(serviceGroupName, serviceNameSet);
                }
                if(serviceNameSet.contains(serviceName))
                {
                    logger.error("Should not contain the ZNodeServiceData: " + value.toString());
                    continue;
                }
                logger.info("Service instance is on line: " + value.toString());
                serviceNameSet.add(serviceName);
            }
            for(String childNode : delZNodeSet)
            {
                ZNodeData value = onLineServiceNameToServiceDataMap.get(childNode);
                if (value == null)
                {
                    logger.error("Should not contain the ZNodeServiceData: " + childNode);
                    continue;
                }
                String serviceGroupName = value.getServiceGroupName();
                String serviceName = value.getServiceName();
                if(!onLineServiceGroupToServiceNameSetMap.containsKey(childNode))
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
                if(serviceNameSet.size() == 0)
                {
                    logger.info("Service group is off line: " + serviceGroupName);
                    onLineServiceGroupToServiceNameSetMapCopy.remove(serviceGroupName);
                }
            }
            
            Map<String, ZNodeData> createdZnodeToServiceNameCopy = new HashMap<String, ZNodeData>(onLineServiceNameToServiceDataMap);
            createdZnodeToServiceNameCopy.putAll(addZNodeMap);
            for(String childNode : delZNodeSet)
            {
                createdZnodeToServiceNameCopy.remove(childNode);
            }
        
            onLineServiceGroupToServiceNameSetMap = onLineServiceGroupToServiceNameSetMapCopy;
            onLineServiceNameToServiceDataMap = createdZnodeToServiceNameCopy;
        }
        finally
        {
            lock.unlock();
        }
    }
    
    private void updateReadyService() throws KeeperException, InterruptedException
    {
        
        Set<String> addReadyServiceGroup = new HashSet<String>();
        Set<String> delReadyServiceGroup = new HashSet<String>();
        //copy on write
        lock.lock();
        try
        {
            Set<String> readyServiceGroupSetCopy = new HashSet<String>();
            Set<String> onLineServiceGroupSet = onLineServiceGroupToServiceNameSetMap.keySet();
            for(Map.Entry<String, Set<String>> entry : dependenceMap.entrySet())
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
                }
            }
            
            for(String serviceGroupName : readyServiceGroupSet)
            {
                if(!readyServiceGroupSetCopy.contains(serviceGroupName))
                {
                    delReadyServiceGroup.add(serviceGroupName);
                }
            }
            readyServiceGroupSet = readyServiceGroupSetCopy;
        }
        finally
        {
            lock.unlock();
        }
        if(!addReadyServiceGroup.isEmpty() || !delReadyServiceGroup.isEmpty())
        {
            updateDependenceZnode(addReadyServiceGroup, delReadyServiceGroup);
        }
    }
    
    private void updateDependenceZnode(Set<String> addReadyServiceGroup, Set<String> delReadyServiceGroup) throws KeeperException, InterruptedException
    {
        for(String serviceGroup : addReadyServiceGroup)
        {
            String path = this.depZnodePath + "/" + serviceGroup;
            if(ZkUtil.checkZnodeExist(zk, path))
            {
                logger.error("Dependence child node should not be exist: " + path);
                continue;
            }
            ZNodeDependenceData zNodeDependenceData = getDependencyData(serviceGroup);
            ZkUtil.createEphemeralZNode(zk, path, zNodeDependenceData.createJsonObject());
        }
        
        for(String serviceGroup : delReadyServiceGroup)
        {
            String path = this.depZnodePath + "/" + serviceGroup;
            if(!ZkUtil.checkZnodeExist(zk, path))
            {
                logger.error("Dependence child should be exist: " + path);
                continue;
            }
            ZkUtil.deleteZnode(zk, path);
        }
    }
    
    private ZNodeDependenceData getDependencyData(String serviceGroup)
    {
        Map<String, Set<ZNodeData>> serviceDataMap = new HashMap<String, Set<ZNodeData>>();
        Set<String> dependenceServiceGroupSet = dependenceMap.get(serviceGroup);
        for(String dependenceServiceGroup : dependenceServiceGroupSet)
        {
            Set<String> serviceNameSet = onLineServiceGroupToServiceNameSetMap.get(dependenceServiceGroup);
            Set<ZNodeData> serviceDataSet = new HashSet<ZNodeData>();
            for(String serviceName : serviceNameSet)
            {
                serviceDataSet.add(onLineServiceNameToServiceDataMap.get(serviceName));
            }
            serviceDataMap.put(dependenceServiceGroup, serviceDataSet);
        }
        return new ZNodeDependenceData(serviceDataMap);
    }
    
    private String getRegZnodePathForChildNode(String childNode)
    {
        return this.regZnodePath + "/" + childNode;
    }

    @Override
    public boolean isServiceReady(String serviceName) throws ServiceUnavailableException
    {
        if (!isRunning)
        {
            throw new ServiceUnavailableException();
        }
        return readyServiceGroupSet.contains(serviceName);
    }

    @Override
    public boolean isActiveManageService() throws ServiceUnavailableException
    {
        if (!isRunning)
        {
            throw new ServiceUnavailableException();
        }
        if (statue.get() == CommonConstants.SERVICE_ACTIVE)
        {
            return true;
        }
        else
        {
            return false;
        }
    }

    @Override
    public void stopManageService()
    {
        if(isRunning)
        {
            stop(); 
        }
    }

    @Override
    public void startManageService()
    {
        if(!isRunning)
        {
            start();
        }
    }
    
    /**
     * 
     * 当kafka设备正常时，就创建一个kafka的child node，如果kafka异常了，就去掉该node.只有Active的Orchestration才会启动monitor的
     * 
     * @author yanbinwa
     *
     */
    class KafkaMonitor implements Callback
    {
        String kafkaHostPort = null;
        String monitorTopic = null;
        String kafkaZnodeChildPath = null;
        KafkaProducer<Object, Object> producer = null;
        boolean isKafkaMonitorRunning = false;
        boolean isTimeout = false;
        Thread monitorThread = null;
        ZNodeServiceData kafkaData = null;
        
        public KafkaMonitor(Map<String, String>kafkaProperites)
        {
            kafkaHostPort = kafkaProperites.get(OrchestrationService.KAFKA_HOSTPORT_KEY);
            if (kafkaHostPort == null)
            {
                logger.error("Kafka host port should not be null");
                return;
            }
            monitorTopic = kafkaProperites.get(OrchestrationService.KAFKA_TEST_TOPIC_KEY);
            if (monitorTopic == null)
            {
                logger.error("Kafka monitor topic should not be null");
                return;
            }
            kafkaZnodeChildPath = kafkaProperites.get(OrchestrationService.ZNODE_KAFKACHILDPATH);
            if (kafkaZnodeChildPath == null)
            {
                logger.error("Kafka znode path should not be null");
                return;
            }
            kafkaData = new ZNodeServiceData("kafkaIp", "kafka", "kafka", -1, "kafkaUrl");
        }
        
        public void start()
        {
            if (!isKafkaMonitorRunning)
            {
                isKafkaMonitorRunning = true;
                buildKafkaProducer();
                monitorThread = new Thread(new Runnable(){

                    @Override
                    public void run()
                    {
                        monitorKafka();
                    }
                    
                });
                monitorThread.start();
            }
            else
            {
                logger.info("kafka monitor has already started");
            }
        }
        
        private void monitorKafka()
        {
            logger.info("Start monitor kafka");
            while(isKafkaMonitorRunning)
            {
                logger.trace("Try to send check msg ");
                ProducerRecord<Object, Object> record = new ProducerRecord<Object, Object>(monitorTopic, "Check msg");
                producer.send(record, this);
                //连接错误，需要重连
                if (isTimeout)
                {
                    //这里说明kafka出现问题，删除Kafka的node
                    producer.close();
                    producer = null;
                    try
                    {
                        if (ZkUtil.checkZnodeExist(zk, kafkaZnodeChildPath))
                        {
                            ZkUtil.deleteZnode(zk, kafkaZnodeChildPath);
                            logger.info("Delete kafka znode: " + kafkaZnodeChildPath);
                        }
                        Thread.sleep(OrchestrationService.KAFKA_PRODUCER_TIMEOUT_SLEEP);
                    } 
                    catch (InterruptedException e)
                    {
                        if(!isRunning)
                        {
                            logger.info("Close the kafka producer worker thread");
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
                    buildKafkaProducer();
                    continue;
                }
                else
                {
                    //这里要创建Kafka node，通知kafka已经上线了
                    try
                    {
                        if (!ZkUtil.checkZnodeExist(zk, kafkaZnodeChildPath))
                        {
                            ZkUtil.createEphemeralZNode(zk, kafkaZnodeChildPath, kafkaData.createJsonObject());
                            logger.info("Create kafka znode: " + kafkaZnodeChildPath);
                        }
                        Thread.sleep(OrchestrationService.KAFKA_PRODUCER_CHECK_INTERVAL);
                    }
                    catch (InterruptedException e)
                    {
                        if(!isRunning)
                        {
                            logger.info("Close the kafka producer worker thread");
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
                }
            }
        }
        
        public void stop()
        {
            if (isKafkaMonitorRunning)
            {
                isKafkaMonitorRunning = false;
                monitorThread.interrupt();
                try
                {
                    if (ZkUtil.checkZnodeExist(zk, kafkaZnodeChildPath))
                    {
                        ZkUtil.deleteZnode(zk, kafkaZnodeChildPath);
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
            }
            else
            {
                logger.info("kafka monitor has already stopped");
            }
        }

        private void buildKafkaProducer()
        {
            Properties props = new Properties();
            props.put("bootstrap.servers", kafkaHostPort);
            props.put("acks", "all");
            props.put("retries", 0);
            props.put("batch.size", 200);
            props.put("linger.ms", 1);
            props.put("buffer.memory", 10000);
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("max.block.ms", 5000);
            
            producer = new KafkaProducer<Object, Object>(props);
        }
        
        
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception)
        {
            logger.trace("Metadata: " + metadata);
            if (exception instanceof TimeoutException)
            {
                isTimeout = true;
                logger.error("Exception: " + exception);
            }
            else
            {
                isTimeout = false;
            }
        }
    }
}
