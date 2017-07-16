package yanbinwa.iOrchestration.management;

import yanbinwa.common.iInterface.ServiceLifeCycle;

public interface MonitorManagement extends ServiceLifeCycle 
{
    public static final String MONITOR_REDIS_HOST_KEY = "redisHost";
    public static final String MONITOR_REDIS_PORT_KEY = "redisPort";
    public static final String MONITOR_REDIS_MAX_TOTAL_KEY = "maxTotal";
    public static final String MONITOR_REDIS_MAX_IDEL_KEY = "maxIdel";
    public static final String MONITOR_REDIS_MAX_WAIT_KEY = "maxWait";
    public static final String MONITOR_REDIS_TEST_ON_BORROW_KEY = "testOnBorrow";
    
    public static final int MONITOR_REDIS_MAX_TOTAL_DEFAULT = 1;
    public static final int MONITOR_REDIS_MAX_IDEL_DEFAULT = 1;
    public static final long MONITOR_REDIS_MAX_WAIT_DEFAULT = -1;
    public static final boolean MONITOR_REDIS_TEST_ON_BORROW_DEFAULT = true;
    
    public static final String MONITOR_REDIS_TEST_KEY = "redisMonitorKey";
    public static final String MONITOR_REDIS_TEST_VALUE = "test";
    
    public static final int MONITOR_REDIS_TIMEOUT_SLEEP = 5 * 1000;
    public static final int REDIS_CHECK_INTERVAL = 5 * 1000;
    
    public static final String REDIS_MONITOR_KEY = "RedisTestKey";
    public static final String REDIS_MONITOR_VALUE = "RedisMonitorValue";
    public static final int REDIS_MONITOR_EXPIRE_TIME = 1;
}
