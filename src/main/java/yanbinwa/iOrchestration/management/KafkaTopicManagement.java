package yanbinwa.iOrchestration.management;

import java.util.Map;
import java.util.Set;

import yanbinwa.common.zNodedata.ZNodeServiceData;

public interface KafkaTopicManagement
{
    void updataKafkaTopicMapping(Map<String, ZNodeServiceData> addZNodeMap, Map<String, ZNodeServiceData> delZNodeMap);
        
    Map<String, Map<String, Set<Integer>>> getTopicGroupToTopicToPartitionKeyMappingByServiceGroup(String serviceName);
    
    void reset();
}
