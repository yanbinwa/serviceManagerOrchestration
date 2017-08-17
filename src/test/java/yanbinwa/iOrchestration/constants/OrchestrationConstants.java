package yanbinwa.iOrchestration.constants;

public class OrchestrationConstants
{
    public static final String TEST_SERVER_IP = "192.168.56.11";
    public static final int TEST_ZOOKEEPER_PORT = 2181;
    public static final int TEST_KAFKA_PORT = 9091;
    public static final int TEST_REDIS_PORT = 6379;
    public static final String TEST_ZOOKEEPERHOSTPORT = TEST_SERVER_IP + ":" + TEST_ZOOKEEPER_PORT;
    public static final String TEST_KAFKAHOSTPORT = TEST_SERVER_IP + ":" + TEST_ZOOKEEPER_PORT;
    public static final String TEST_REDISHOSTPORT = TEST_SERVER_IP + ":" + TEST_REDIS_PORT;
}
