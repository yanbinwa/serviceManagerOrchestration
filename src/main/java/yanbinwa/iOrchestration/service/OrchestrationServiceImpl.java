package yanbinwa.iOrchestration.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.json.JSONObject;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Service;

import yanbinwa.common.configClient.ConfigCallBack;
import yanbinwa.common.configClient.ConfigClient;
import yanbinwa.common.configClient.ConfigClientImpl;
import yanbinwa.common.configClient.ServiceConfigState;
import yanbinwa.common.constants.CommonConstants;
import yanbinwa.common.exceptions.ServiceUnavailableException;
import yanbinwa.common.utils.JsonUtil;
import yanbinwa.common.utils.MapUtil;
import yanbinwa.common.utils.ZkUtil;
import yanbinwa.common.zNodedata.ZNodeData;
import yanbinwa.common.zNodedata.ZNodeDataUtil;
import yanbinwa.common.zNodedata.ZNodeDependenceData;
import yanbinwa.common.zNodedata.ZNodeServiceData;
import yanbinwa.common.zNodedata.ZNodeServiceDataImpl;
import yanbinwa.iOrchestration.management.DependencyManagement;
import yanbinwa.iOrchestration.management.DependencyManagementImpl;
import yanbinwa.iOrchestration.management.KafkaMonitorManagementImpl;
import yanbinwa.iOrchestration.management.MonitorManagement;
import yanbinwa.iOrchestration.management.RedisMonitorManagementImpl;

/**
 * 该服务包括:
 * 1.对于Zookeeper连接以及操作的管理
 * 
 * 2.对于其它service ZNode监控
 * 
 * 3.对于Action-Standby的管理
 * 
 * @author yanbinwa
 */

@Service("orchestrationService")
@EnableAutoConfiguration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "orchestration")
public class OrchestrationServiceImpl implements OrchestrationService
{
    
    private static final Logger logger = Logger.getLogger(OrchestrationServiceImpl.class);
    
    private Map<String, String> serviceDataProperties;
    private Map<String, String> zNodeInfoProperties;
    
    
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
    
    private String dependencyProperties = null;
    private Map<String, Object> monitorProperties = null;
    
    DependencyManagement dependencyManagement = null;
    Map<String, MonitorManagement> serviceMonitorMap = new HashMap<String, MonitorManagement>();
    
    ZNodeServiceData serviceData = null;
    
    String configZnodePath = null;
    String regZnodePath = null;
    String regZnodeChildPath = null;
    String depZnodePath = null;
    String zookeeperHostport = null;
    
    volatile boolean isConfiged = false;
    volatile boolean isRunning = false;
    
    /** 主要处理zookeeper事件的方法*/
    Thread zookeeperThread = null;
    
    Thread zookeeperSync = null;
    
    /** Zookeeper connection */
    ZooKeeper zk = null;
    
    /** 存放监听到的Zookeeper信息 */
    BlockingQueue<WatchedEvent> zookeeperEventQueue = new LinkedBlockingQueue<WatchedEvent>();
    
    Watcher zkWatcher = new ZkWatcher();
    
    /** active or standby */
    volatile int serviceStatue = CommonConstants.SERVICE_STANDBY;
    
    private ConfigClient configClient = null;
    
    ConfigCallBack configCallBack = new OrchestrationConfigCallBack();
    
    /** config update lock */
    ReentrantLock lock = new ReentrantLock();
    
    @Override
    public void afterPropertiesSet() throws Exception
    {
        /* 获取服务自身信息 */
        String serviceGroup = serviceDataProperties.get(OrchestrationServiceImpl.SERVICE_SERVICEGROUPNAME);
        String serviceName = serviceDataProperties.get(OrchestrationServiceImpl.SERVICE_SERVICENAME);
        String ip = serviceDataProperties.get(OrchestrationServiceImpl.SERVICE_IP);
        String portStr = serviceDataProperties.get(OrchestrationServiceImpl.SERVICE_PORT);
        int port = Integer.parseInt(portStr);
        String rootUrl = serviceDataProperties.get(OrchestrationServiceImpl.SERVICE_ROOTURL);
        serviceData = new ZNodeServiceDataImpl(ip, serviceGroup, serviceName, port, rootUrl);
        
        /* Zookeeper操作相关信息 */
        regZnodePath = zNodeInfoProperties.get(OrchestrationServiceImpl.ZNODE_REGPATH);
        regZnodeChildPath = zNodeInfoProperties.get(OrchestrationServiceImpl.ZNODE_REGCHILDPATH);
        depZnodePath = zNodeInfoProperties.get(OrchestrationServiceImpl.ZNODE_DEPPATH);
        zookeeperHostport = zNodeInfoProperties.get(OrchestrationServiceImpl.ZK_HOSTPORT);
        
        configClient = new ConfigClientImpl(serviceData, configCallBack, zookeeperHostport, zNodeInfoProperties);
        start();
    }

    /**
     * Start 会创建ConfigClient，这里应该有一个ServiceState状态机，Init，Configed，IsReady是获取Config后的状态.
     * Stop 会
     */
    @Override
    public void start()
    {
        if(!isRunning)
        {
            isRunning = true;
            configClient.start();
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
            isRunning = false;
            if (configClient != null)
            {
                configClient.stop();
            }
        }
        else
        {
            logger.info("Orchestration serivce has ready stopped...");
        }
    }

    @Override
    public JSONObject getReadyService() throws ServiceUnavailableException
    {
        if (!isServiceReadyToWork())
        {
            throw new ServiceUnavailableException();
        }
        return dependencyManagement.getReadyService();
    }

    @Override
    public boolean isServiceReady(String serviceName) throws ServiceUnavailableException
    {
        if (!isServiceReadyToWork())
        {
            throw new ServiceUnavailableException();
        }
        return dependencyManagement.isServiceReady(serviceName);
    }

    @Override
    public boolean isActiveManageService() throws ServiceUnavailableException
    {
        if (!isServiceReadyToWork())
        {
            throw new ServiceUnavailableException();
        }
        return serviceStatue == CommonConstants.SERVICE_ACTIVE;
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
    
    @Override
    public ZNodeServiceData getRegZnodeData(String zNodeName) throws KeeperException, InterruptedException, ServiceUnavailableException
    {
        if (!isServiceReadyToWork())
        {
            throw new ServiceUnavailableException();
        }
        if (zk == null)
        {
            logger.error("Zookeeper is null");
            return null;
        }
        String zNodePath = getRegZnodePathForChildNode(zNodeName);
        try
        {
            JSONObject dataObj = ZkUtil.getData(zk, zNodePath);
            return ZNodeDataUtil.getZnodeData(dataObj);
        } 
        catch (KeeperException e)
        {
            //如果这时子节点被删除了，这里就跳过，之后会有watcher来处理的
            if(e.code() == KeeperException.Code.NODEEXISTS)
            {
                logger.info("Child node has been deleted in concurrent " + e.getMessage());
                return null;
            }
            else
            {
                logger.error("Create register or dependence node fail " + e.getMessage());
                throw e;
            }
        }
    }

    @Override
    public boolean isRegZnodeExist(String serviceName) throws KeeperException, InterruptedException, ServiceUnavailableException
    {
        if (!isServiceReadyToWork())
        {
            throw new ServiceUnavailableException();
        }
        String path = getRegZnodePathForChildNode(serviceName);
        return ZkUtil.checkZnodeExist(zk, path);
    }

    @Override
    public boolean isDepZnodeExist(String serviceGroupName) throws KeeperException, InterruptedException, ServiceUnavailableException
    {
        if (!isServiceReadyToWork())
        {
            throw new ServiceUnavailableException();
        }
        String path = getDepZnodePathForChildNode(serviceGroupName);
        return ZkUtil.checkZnodeExist(zk, path);
    }

    @Override
    public void createDepZnode(String serviceGroupName, ZNodeDependenceData zNodeDependenceData) throws KeeperException, InterruptedException, ServiceUnavailableException
    {
        if (!isServiceReadyToWork())
        {
            throw new ServiceUnavailableException();
        }
        String path = getDepZnodePathForChildNode(serviceGroupName);
        ZkUtil.createEphemeralZNode(zk, path, zNodeDependenceData.createJsonObject());
    }

    @Override
    public void updateDepZnode(String serviceGroupName, ZNodeDependenceData zNodeDependenceData) throws KeeperException, InterruptedException, ServiceUnavailableException
    {
        if (!isServiceReadyToWork())
        {
            throw new ServiceUnavailableException();
        }
        String path = getDepZnodePathForChildNode(serviceGroupName);
        ZkUtil.setData(zk, path, zNodeDependenceData.createJsonObject());
    }
    
    @Override
    public void deleteRegZnode(String serviceName) throws InterruptedException, KeeperException, ServiceUnavailableException
    {
        if (!isServiceReadyToWork())
        {
            throw new ServiceUnavailableException();
        }
        String path = getRegZnodePathForChildNode(serviceName);
        ZkUtil.deleteZnode(zk, path);
    }
    
    @Override
    public void createRegZnode(String serviceName, ZNodeData zNodeData) throws KeeperException, InterruptedException, ServiceUnavailableException
    {
        if (!isServiceReadyToWork())
        {
            throw new ServiceUnavailableException();
        }
        String path = getRegZnodePathForChildNode(serviceName);
        ZkUtil.createEphemeralZNode(zk, path, zNodeData.createJsonObject());
    }
    

    @Override
    public void deleteDepZnode(String serviceName) throws InterruptedException, KeeperException, ServiceUnavailableException
    {
        if (!isServiceReadyToWork())
        {
            throw new ServiceUnavailableException();
        }
        String path = getDepZnodePathForChildNode(serviceName);
        ZkUtil.deleteZnode(zk, path);
    }
    

    @Override
    public void startWork()
    {
        logger.info("Start work orchestration serivce ...");
        /** 连接Zookeeper，创建相应的Znode，并监听其它服务创建的Znode */
        init();
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

    @Override
    public void stopWork()
    {
        logger.info("Stop work orchestration serivce ...");

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
        serviceStatue = CommonConstants.SERVICE_STANDBY;
        stopMonitorManagement();
        dependencyManagement.reset();
        reset();
    }
    
    private void init()
    {
        /* 将依赖传入到DependencyManagement */
        JSONObject serviceDependencesObj = new JSONObject(dependencyProperties);
        dependencyManagement = new DependencyManagementImpl(this, serviceDependencesObj);
        
        /* 创建Monitor */
        resetMonitorServiceMap();
        buildMonitorServiceMap();
    }
    
    private void reset()
    {
        dependencyProperties = null;
        monitorProperties = null;
    }
    
    @SuppressWarnings("unchecked")
    private void buildMonitorServiceMap()
    {
        if (monitorProperties != null)
        {
            for(Map.Entry<String, Object> entry : monitorProperties.entrySet())
            {
                String serviceName = entry.getKey();
                if (serviceMonitorMap.containsKey(serviceName))
                {
                    logger.error("Should not add monitor for service " + serviceName + " more than one time. "
                            + "The properties is: " + entry.getValue());
                    continue;
                }
                Object monitorPropertyObj = entry.getValue();
                if (monitorPropertyObj == null || !(monitorPropertyObj instanceof Map))
                {
                    logger.error("Monitor property for service " + serviceName + " is null or not a map"
                            + "The properties is: " + entry.getValue());
                    continue;
                }
                MonitorManagement monitorManagement = null;
                switch(serviceName)
                {
                case MONITOR_KAFKA_KEY:
                    monitorManagement = new KafkaMonitorManagementImpl((Map<String, Object>)monitorPropertyObj, this);
                    break;
                case MONITOR_REDIS_KEY:
                    monitorManagement = new RedisMonitorManagementImpl((Map<String, Object>)monitorPropertyObj, this);
                    break;
                }
                if (monitorManagement != null)
                {
                    logger.info("Add service monitor for " + serviceName);
                    serviceMonitorMap.put(serviceName, monitorManagement);
                }
            }
        }
    }
    
    private void resetMonitorServiceMap()
    {
        serviceMonitorMap.clear();
    }

    private void zookeeperEventHandler()
    {
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
                serviceStatue = CommonConstants.SERVICE_ACTIVE;
                
            }
            else if(!ZkUtil.checkZnodeExist(zk, regZnodeChildPath))
            {
                //这里有两种情况，一种是有其它的服务已经创建了父目录，但还没来得及创建子目录，另一种情况是之前服务异常退出，没有来得及删除
                Thread.sleep(OrchestrationServiceImpl.ZKNODE_REGCHILDPATH_WAITTIME);
                //等待建立子目录，但是还没有建立说明是之前异常退出时遗留的
                if(!ZkUtil.checkZnodeExist(zk, regZnodeChildPath))
                {
                    setUpZnodeForActive();
                    serviceStatue = CommonConstants.SERVICE_ACTIVE;
                }
            }
            ZkUtil.watchZnodeChildeChange(zk, regZnodePath, zkWatcher);
            while(isRunning)
            {
                if (serviceStatue == CommonConstants.SERVICE_STANDBY)
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
    
    private void handerZookeeperEventAsStandby()
    {
        logger.info("Starting handler zookeepr event for standby... ");
        try
        {
            ZkUtil.watchZnodeChildeChange(zk, regZnodePath, zkWatcher);
            while(isRunning && serviceStatue  == CommonConstants.SERVICE_STANDBY)
            {
                WatchedEvent event = zookeeperEventQueue.poll(OrchestrationServiceImpl.ZKEVENT_QUEUE_TIMEOUT, 
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
                        serviceStatue = CommonConstants.SERVICE_ACTIVE;
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
            if(e instanceof KeeperException.SessionExpiredException)
            {
                logger.info("zookeepr session is expired, need to reconnect");
                if (zk != null)
                {
                    try
                    {
                        ZkUtil.closeZk(zk);
                        Thread.sleep(ZK_RECONNECT_INTERVAL);
                    } 
                    catch (InterruptedException e1)
                    {
                        //do nothing
                    }
                }
                serviceStatue = CommonConstants.SERVICE_STANDBY;
                stopMonitorManagement();
                dependencyManagement.reset();
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
                ZNodeServiceData data = new ZNodeServiceDataImpl(ZkUtil.getData(zk, regZnodePath));
                //被其它的device抢先设置了
                if(!data.equals(this.serviceData))
                {
                    serviceStatue = CommonConstants.SERVICE_STANDBY;
                    return;
                }
            }
            //开始Monitor服务
            startMonitorManagement();
            
            while(isRunning && serviceStatue == CommonConstants.SERVICE_ACTIVE)
            {
                WatchedEvent event = zookeeperEventQueue.poll(OrchestrationServiceImpl.ZKEVENT_QUEUE_TIMEOUT, 
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
                        Thread.sleep(ZK_RECONNECT_INTERVAL);
                    } 
                    catch (InterruptedException e1)
                    {
                        //do nothing
                    }
                }
                serviceStatue = CommonConstants.SERVICE_STANDBY;
                stopMonitorManagement();
                dependencyManagement.reset();
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
        catch (ServiceUnavailableException e)
        {
            logger.info("orchestrationService is stop");
            return;
        }
        logger.info("End handler zookeepr event for active... ");
    }
    
    private void startMonitorManagement()
    {
        for (Map.Entry<String, MonitorManagement> entry : serviceMonitorMap.entrySet())
        {
            logger.info("Monitor service " + entry.getKey() + " start ...");
            entry.getValue().start();
        }
    }
    
    private void stopMonitorManagement()
    {
        for (Map.Entry<String, MonitorManagement> entry : serviceMonitorMap.entrySet())
        {
            logger.info("Monitor service " + entry.getKey() + " stop ...");
            entry.getValue().stop();
        }
    }
    
    private void handerChildNodeChange(String parentPath) throws KeeperException, InterruptedException, ServiceUnavailableException
    {
        String path = parentPath;
        //获取的是childZnode的名字，而不是path
        List<String> childList = ZkUtil.getChildren(zk, path);
        //去掉orchestartion自己创建的子node
        childList.remove(getRegZnodeChildName());
        
        logger.info("Get current children list: " + childList);
        this.dependencyManagement.updateServiceDependence(childList);
    }
    
    private String getRegZnodeChildName()
    {
        int index = regZnodeChildPath.lastIndexOf("/");
        return regZnodeChildPath.substring(index + 1);
    }
    
    private String getRegZnodePathForChildNode(String zNodeName)
    {
        return this.regZnodePath + "/" + zNodeName;
    }
    
    private String getDepZnodePathForChildNode(String zNodeName)
    {
        return this.depZnodePath + "/" + zNodeName;
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
                Thread.sleep(OrchestrationServiceImpl.ZK_SYNC_INTERVAL);
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
    
    /**
     *  如果serviceConfigProperties内容不一致，就要停掉之前的工作，进行更新，再启动（需要必须停止吗）
     */
    @SuppressWarnings("unchecked")
    private void updateServiceConfigProperties(JSONObject serviceConfigPropertiesObj)
    {
        if (!serviceConfigPropertiesObj.has(CommonConstants.SERVICE_DEPENDENCE_PROPERTIES_KEY))
        {
            logger.error("serviceConfigPropertiesObj does not contains dependencyProperties; serviceConfigPropertiesObj: " + serviceConfigPropertiesObj);
        }
        String dependencyPropertiesTmp = serviceConfigPropertiesObj.getString(CommonConstants.SERVICE_DEPENDENCE_PROPERTIES_KEY);
        if (dependencyPropertiesTmp == null)
        {
            logger.error("dependencyPropertiesTmp should not be null");
            return;
        }
        
        if (!serviceConfigPropertiesObj.has(CommonConstants.SERVICE_MONITOR_PROPERTIES_KEY))
        {
            logger.error("serviceConfigPropertiesObj does not contains monitorProperties; serviceConfigPropertiesObj: " + serviceConfigPropertiesObj);
        }
        JSONObject monitorPropertiesTmpObj = serviceConfigPropertiesObj.getJSONObject(CommonConstants.SERVICE_MONITOR_PROPERTIES_KEY);
        
        Map<String, Object> monitorPropertiesTmp = (Map<String, Object>) JsonUtil.JsonStrToMap(monitorPropertiesTmpObj.toString());
        if (monitorPropertiesTmp == null)
        {
            logger.info("dependencyProperties is null");
            monitorPropertiesTmp = new HashMap<String, Object>();
        }
        
        boolean ret = compareAndUpdataServiceConfigProperties(dependencyPropertiesTmp, monitorPropertiesTmp);
        if (ret)
        {
            logger.info("Update the serviceProperties for Orchestration.");
            logger.info("dependencyPropertiesTmp is: " + dependencyPropertiesTmp + "; monitorPropertiesTmp is: " + monitorPropertiesTmp);
            if (isConfiged)
            {
                stopWork();
            }
            isConfiged = true;
            startWork();
        }
    }
    
    /**
     * 比较配置是否一致，尤其是monitor配置的比较
     * 
     * @param dependencyPropertiesTmp
     * @param monitorPropertiesTmp
     * @return
     */
    private boolean compareAndUpdataServiceConfigProperties(String dependencyPropertiesTmp, Map<String, Object> monitorPropertiesTmp)
    {
        lock.lock();
        try
        {
            if (dependencyProperties == null || monitorProperties == null)
            {
                dependencyProperties = dependencyPropertiesTmp;
                monitorProperties = monitorPropertiesTmp;
                return true;
            }
            boolean isChanged = false;
            if (!dependencyProperties.equals(dependencyPropertiesTmp))
            {
                isChanged = true;
                dependencyProperties = dependencyPropertiesTmp;
            }
            
            if (!MapUtil.compareMap(monitorProperties, monitorPropertiesTmp))
            {
                isChanged = true;
                monitorProperties = monitorPropertiesTmp;
            }
            return isChanged;
        }
        finally
        {
            lock.unlock();
        }
    }
    
    private boolean isServiceReadyToWork()
    {
        return isRunning && isConfiged;
    }
    
    class ZkWatcher implements Watcher
    {
        @Override
        public void process(WatchedEvent event)
        {
            zookeeperEventQueue.offer(event);
        } 
    }
    
    class OrchestrationConfigCallBack implements ConfigCallBack
    {
        @Override
        public void handleServiceConfigChange(ServiceConfigState state)
        {
            logger.info("Service config state is: " + state);
            if (state == ServiceConfigState.CREATED || state == ServiceConfigState.CHANGED)
            {
                JSONObject serviceConfigPropertiesObj = configClient.getServiceConfigProperties();
                if (ZNodeDataUtil.validateServiceConfigProperties(serviceData, serviceConfigPropertiesObj))
                {
                    updateServiceConfigProperties(serviceConfigPropertiesObj);
                }
                else
                {
                    logger.error("Un valid service config properties: " + serviceConfigPropertiesObj);
                }
            }
            else if (state == ServiceConfigState.DELETED || state == ServiceConfigState.CLOSE)
            {
                if (isConfiged)
                {
                    stopWork();
                }
                isConfiged = false;
            }
            else
            {
                logger.error("Unknow ServiceConfigState: " + state);
            }
        }
    }
}
