//package yanbinwa.iOrchestration.service;
//
//import org.json.JSONObject;
//import org.springframework.beans.factory.InitializingBean;
//
//import yanbinwa.common.iInterface.ServiceLifeCycle;
//import yanbinwa.iOrchestration.exception.ServiceUnavailableException;
//
//public interface OrchestrationService extends InitializingBean, ServiceLifeCycle
//{
//    public static final String ZNODE_REGPATH = "regZnodePath";
//    public static final String ZNODE_REGCHILDPATH = "regZnodeChildPath";
//    public static final String ZNODE_DEPPATH = "depZnodePath";
//    public static final String ZK_HOSTPORT = "zookeeperHostport";
//    
//    public static final int ZKEVENT_QUEUE_TIMEOUT = 5000;
//    
//    public static final String SERVICE_IP = "ip";
//    public static final String SERVICE_SERVICEGROUPNAME = "serviceGroupName";
//    public static final String SERVICE_SERVICENAME = "serviceName";
//    public static final String SERVICE_PORT = "port";
//    public static final String SERVICE_ROOTURL = "rootUrl";
//    
//    public static final int ZK_SYNC_INTERVAL = 60 * 1000;
//    public static final int ZKNODE_REGCHILDPATH_WAITTIME = 1000;
//    public static final int ZK_WAIT_INTERVAL = 10 * 1000;
//    
//    public static final String KAFKA_HOSTPORT_KEY = "kafkaHostPort";
//    public static final String KAFKA_TEST_TOPIC_KEY = "testTopic";
//    public static final String ZNODE_KAFKACHILDPATH = "kafkaZnodeChildPath";
//    public static final int KAFKA_PRODUCER_TIMEOUT_SLEEP = 15 * 1000;
//    public static final int KAFKA_PRODUCER_CHECK_INTERVAL = 15 * 1000;
//        
//    JSONObject getReadyService() throws ServiceUnavailableException;
//    
//    boolean isServiceReady(String serviceName) throws ServiceUnavailableException;
//    
//    boolean isActiveManageService() throws ServiceUnavailableException;
//    
//    void stopManageService();
//    
//    void startManageService();
//}
