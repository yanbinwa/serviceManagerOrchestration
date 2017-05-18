package yanbinwa.iOrchestration.service;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

public class OrchestrationServiceImplTest
{

    @Test
    public void test()
    {
        Map<String, Set<String>> dependenceMap = new HashMap<String, Set<String>>();
        Set<String> dependenceForA = new HashSet<String>();
        dependenceForA.add("ServiceB");
        dependenceForA.add("ServiceC");
        dependenceMap.put("ServiceA", dependenceForA);
        
        Set<String> dependenceForB = new HashSet<String>();
        dependenceForB.add("ServiceC");
        dependenceMap.put("ServiceB", dependenceForB);
        
        Set<String> dependenceForC = new HashSet<String>();
        dependenceMap.put("ServiceC", dependenceForC);

        JSONObject ret = new JSONObject();
        for(Map.Entry<String, Set<String>> entry : dependenceMap.entrySet())
        {
            String key = entry.getKey();
            boolean hasKey = ret.has(key);
            if(!hasKey)
            {
                JSONArray tmp = new JSONArray();
                ret.put(key, tmp);
            }
            JSONArray array = ret.getJSONArray(key);
            for(String serviceName : entry.getValue())
            {
                array.put(serviceName);
            }
        }
        System.out.println(ret.toString());
    }

}
