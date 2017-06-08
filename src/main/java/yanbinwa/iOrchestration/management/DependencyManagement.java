package yanbinwa.iOrchestration.management;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.json.JSONObject;

import yanbinwa.iOrchestration.exception.ServiceUnavailableException;

public interface DependencyManagement
{   
    public static final String SERVICE_DEPENDENCY_KEY = "serviceDependency";
    public static final String KAFKA_TOPIC_INFO_KEY = "kafkaTopicInfo";
    
    public static final String ADD_SERVICE_GROUPS_KEY = "addServiceGroup";
    public static final String DEL_SERVICE_GROUPS_KEY = "delServiceGroup";
    public static final String CHANGE_SERVICE_GROUPS_KEY = "changeServiceGroup";
    
    /**
     * 当orchestration监听到regZnode下子节点的变动时，会读取全部的child list，交于Dependency来处理
     * 
     * @param childZnodeList
     * @throws InterruptedException 
     * @throws KeeperException 
     * @throws ServiceUnavailableException 
     */
    void updateServiceDependence(List<String> childZnodeList) throws InterruptedException, KeeperException, ServiceUnavailableException;
    
    void reset();
    
    JSONObject getReadyService();
    
    boolean isServiceReady(String serviceName);
}
