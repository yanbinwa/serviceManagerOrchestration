package yanbinwa.iOrchestration.management;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

public class KafkaTopicManagementImplTest
{

    @Test
    public void test()
    {
        List<Integer> avaliablePartitionKeyList = new ArrayList<Integer>();
        Set<Integer> avaliablePartitionKeySet = new HashSet<Integer>();
        for(int i = 0; i < 10; i ++)
        {
            avaliablePartitionKeyList.add(i);
            avaliablePartitionKeySet.add(i);
        }
        for(int i = 0; i < 10; i ++)
        {
            Integer partitionKey = avaliablePartitionKeyList.get(0);
            avaliablePartitionKeyList.remove(0);
            avaliablePartitionKeySet.remove(partitionKey);
        }
        System.out.println("avaliablePartitionKeyList is: " + avaliablePartitionKeyList);
        System.out.println("avaliablePartitionKeySet is: " + avaliablePartitionKeySet);
    }

}
