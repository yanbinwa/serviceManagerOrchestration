package yanbinwa.iOrchestration.management;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class RedisPartitionManagementImplTest
{

    @Test
    public void test()
    {
        List<String> testList = new ArrayList<String>();
        testList.add("wyb");
        testList.add("zcl");
        testList.add("wzy");
        testList.add("wjy");
        System.out.println(testList.subList(3, 4));
    }

}
