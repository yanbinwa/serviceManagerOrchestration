package yanbinwa.iOrchestration.management;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import yanbinwa.common.zNodedata.ZNodeServiceData;
import yanbinwa.common.zNodedata.ZNodeServiceDataImpl;
import yanbinwa.iOrchestration.service.IOrchestrationService;

public class KafkaMonitorManagementImpl implements MonitorManagement, Callback
{
    private static final Logger logger = Logger.getLogger(KafkaMonitorManagementImpl.class);
    
    String kafkaHostPort = null;
    String monitorTopic = null;
    KafkaProducer<Object, Object> producer = null;
    boolean isTimeout = false;
    Thread monitorThread = null;
    ZNodeServiceData kafkaData = null;
    boolean isRunning = false;
    
    IOrchestrationService orchestrationService = null;
    
    public KafkaMonitorManagementImpl(Map<String, String> kafkaProperites, IOrchestrationService orchestrationService)
    {
        this.orchestrationService = orchestrationService;
        
        kafkaHostPort = kafkaProperites.get(IOrchestrationService.KAFKA_HOSTPORT_KEY);
        if (kafkaHostPort == null)
        {
            logger.error("Kafka host port should not be null");
            return;
        }
        monitorTopic = kafkaProperites.get(IOrchestrationService.KAFKA_TEST_TOPIC_KEY);
        if (monitorTopic == null)
        {
            logger.error("Kafka monitor topic should not be null");
            return;
        }
        String serviceGroupName = kafkaProperites.get(IOrchestrationService.SERVICE_SERVICEGROUPNAME);
        if (serviceGroupName == null)
        {
            logger.error("Kafka service group name should not be null");
            return;
        }
        String serviceName = kafkaProperites.get(IOrchestrationService.SERVICE_SERVICENAME);
        if (serviceName == null)
        {
            logger.error("Kafka service name should not be null");
            return;
        }
        kafkaData = new ZNodeServiceDataImpl("kafkaIp", serviceGroupName, serviceName, -1, "kafkaUrl");
    }
    
    @Override
    public void start()
    {
        if (!isRunning)
        {
            isRunning = true;
            buildKafkaProducer();
            monitorThread = new Thread(new Runnable(){

                @Override
                public void run()
                {
                    monitorKafka();
                }
                
            });
            monitorThread.start();
        }
        else
        {
            logger.info("kafka monitor has already started");
        }
    }

    @Override
    public void stop()
    {
        if (isRunning)
        {
            isRunning = false;
            monitorThread.interrupt();
            try
            {
                deleteKafkaRegZNode();
            } 
            catch (KeeperException e)
            {
                e.printStackTrace();
            } 
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
        else
        {
            logger.info("kafka monitor has already stopped");
        }
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception)
    {
        logger.trace("Metadata: " + metadata);
        if (exception instanceof TimeoutException)
        {
            isTimeout = true;
            logger.error("Exception: " + exception);
        }
        else
        {
            isTimeout = false;
        }
    }
    
    private void buildKafkaProducer()
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaHostPort);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 200);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 10000);
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("max.block.ms", 10000);
        
        producer = new KafkaProducer<Object, Object>(props);
    }
    
    private void monitorKafka()
    {
        logger.info("Start monitor kafka");
        while(isRunning)
        {
            logger.trace("Try to send check msg ");
            ProducerRecord<Object, Object> record = new ProducerRecord<Object, Object>(monitorTopic, "Check msg");
            producer.send(record, this);
            //连接错误，需要重连
            if (isTimeout)
            {
                //这里说明kafka出现问题，删除Kafka的node
                producer.close();
                producer = null;
                try
                {
                    deleteKafkaRegZNode();
                    Thread.sleep(IOrchestrationService.KAFKA_PRODUCER_TIMEOUT_SLEEP);
                } 
                catch (InterruptedException e)
                {
                    if(!isRunning)
                    {
                        logger.info("Close the kafka producer worker thread");
                    }
                    else
                    {
                        e.printStackTrace();
                    }
                } 
                catch (KeeperException e)
                {
                    e.printStackTrace();
                }
                buildKafkaProducer();
                continue;
            }
            else
            {
                //这里要创建Kafka node，通知kafka已经上线了
                try
                {
                    createKafkaRegZNode();
                    Thread.sleep(IOrchestrationService.KAFKA_PRODUCER_CHECK_INTERVAL);
                }
                catch (InterruptedException e)
                {
                    if(!isRunning)
                    {
                        logger.info("Close the kafka producer worker thread");
                    }
                    else
                    {
                        e.printStackTrace();
                    }
                } 
                catch (KeeperException e)
                {
                    e.printStackTrace();
                }
            }
        }
    }
    
    private void createKafkaRegZNode() throws KeeperException, InterruptedException
    {
        if (!orchestrationService.isRegZnodeExist(kafkaData.getServiceName()))
        {
            orchestrationService.createRegZnode(kafkaData.getServiceName(), kafkaData);
            logger.info("Create kafka znode: " + kafkaData.getServiceName());
        }
    }
    
    private void deleteKafkaRegZNode() throws KeeperException, InterruptedException
    {
        if (orchestrationService.isRegZnodeExist(kafkaData.getServiceName()))
        {
            orchestrationService.deleteRegZnode(kafkaData.getServiceName());
            logger.info("Delete kafka znode: " + kafkaData.getServiceName());
        }
    }

}
