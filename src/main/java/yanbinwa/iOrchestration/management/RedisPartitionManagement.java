package yanbinwa.iOrchestration.management;

import java.util.Map;
import java.util.Set;

import yanbinwa.common.zNodedata.ZNodeServiceData;

public interface RedisPartitionManagement
{
    public static final String REDIS_SERVICEGROUP_KEY = "redis";
    
    public static final int REDIS_PARTITIONNUM_DEFAULT = 10;
    
    void updataRedisPartitionMapping(Map<String, ZNodeServiceData> addZNodeMap, Map<String, ZNodeServiceData> delZNodeMap);
    
    Map<String, Set<Integer>> getRedisToPartitionKeyMapping();
    
    void reset();
}
