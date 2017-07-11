package yanbinwa.iOrchestration.service;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

public class OrchestrationServiceImplTest
{

    @Test
    public void test()
    {
        JSONObject rootObj = new JSONObject();
        
        JSONObject serviceDependenceObj = new JSONObject();
        
        JSONArray kafkaArray = new JSONArray();
        serviceDependenceObj.put("kafka", kafkaArray);
        
        JSONArray redisArray = new JSONArray();
        serviceDependenceObj.put("redis", redisArray);
        
        JSONArray cacheArray = new JSONArray();
        cacheArray.put("kafka");
        cacheArray.put("redis");
        serviceDependenceObj.put("cache", cacheArray);
        
        JSONArray aggregationArray = new JSONArray();
        aggregationArray.put("kafka");
        aggregationArray.put("cache");
        serviceDependenceObj.put("aggregation", aggregationArray);
        
        JSONArray collectionArray = new JSONArray();
        collectionArray.put("kafka");
        collectionArray.put("aggregation");
        collectionArray.put("cache");
        serviceDependenceObj.put("collection", collectionArray);
        
        rootObj.put("serviceDependency", serviceDependenceObj);
        
        JSONObject kafkaTopicInfoObj = new JSONObject();
        kafkaTopicInfoObj.put("aggregationTopic", 10);
        rootObj.put("kafkaTopicInfo", kafkaTopicInfoObj);
        
        rootObj.put("redisPartitionNum", 10);
        System.out.println(rootObj.toString());
    }
    
    @Test
    public void dependenceTest()
    {
        String dependenceStr = "{\"serviceDependency\":{\"cache\":[\"kafka\"],\"kafka\":[],\"collection\":[\"kafka\",\"cache\"]},\"kafkaTopicInfo\":{\"cacheTopic\":10}}";
        JSONObject obj = new JSONObject(dependenceStr);
        System.out.println(obj.toString());
    }

}
