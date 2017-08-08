package yanbinwa.iOrchestration.management;

import java.util.Map;
import java.util.Set;

import yanbinwa.common.zNodedata.ZNodeServiceData;

public interface KafkaTopicManagement
{
    public static final String ZOOKEEPER_HOST_PORT_KEY = "zookeeperHostport";
    public static final String KAFKA_TOPICS_KEY = "topics";
    
    void updataKafkaTopicMapping(Map<String, ZNodeServiceData> addZNodeMap, Map<String, ZNodeServiceData> delZNodeMap);
        
    Map<String, Map<String, Set<Integer>>> getTopicGroupToTopicToPartitionKeyMappingByServiceGroup(String serviceName);
    
    void reset();
}
