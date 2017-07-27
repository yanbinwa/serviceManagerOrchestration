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

import yanbinwa.common.exceptions.ServiceUnavailableException;
import yanbinwa.common.zNodedata.ZNodeServiceData;
import yanbinwa.common.zNodedata.ZNodeServiceDataImpl;
import yanbinwa.iOrchestration.service.OrchestrationService;

public class KafkaMonitorManagementImpl implements MonitorManagement, Callback
{
    private static final Logger logger = Logger.getLogger(KafkaMonitorManagementImpl.class);
    private static final String KAFKA_SERVICE_NAME = "kafka";
    
    String kafkaHostPort = null;
    String monitorTopic = null;
    KafkaProducer<Object, Object> producer = null;
    boolean isTimeout = false;
    Thread monitorThread = null;
    ZNodeServiceData kafkaData = null;
    boolean isRunning = false;
    
    OrchestrationService orchestrationService = null;
    
    @SuppressWarnings("unchecked")
    public KafkaMonitorManagementImpl(Map<String, Object> kafkaProperites, OrchestrationService orchestrationService)
    {
        this.orchestrationService = orchestrationService;
        Object kafkaProperty = kafkaProperites.get(KAFKA_SERVICE_NAME);
        if (kafkaProperty == null || !(kafkaProperty instanceof Map))
        {
            logger.error("kafkaProperty property should be map " + kafkaProperty);
            return;
        }
        Map<String, String> kafkaPropertyMap = (Map<String, String>)kafkaProperty;
        kafkaHostPort = kafkaPropertyMap.get(OrchestrationService.KAFKA_HOSTPORT_KEY);
        if (kafkaHostPort == null)
        {
            logger.error("Kafka host port should not be null. The kafkaProperites is: " + kafkaProperites);
            return;
        }
        monitorTopic = kafkaPropertyMap.get(OrchestrationService.KAFKA_TEST_TOPIC_KEY);
        if (monitorTopic == null)
        {
            logger.error("Kafka monitor topic should not be null");
            return;
        }
        String serviceGroupName = kafkaPropertyMap.get(OrchestrationService.SERVICE_SERVICEGROUPNAME);
        if (serviceGroupName == null)
        {
            logger.error("Kafka service group name should not be null");
            return;
        }
        String serviceName = kafkaPropertyMap.get(OrchestrationService.SERVICE_SERVICENAME);
        if (serviceName == null)
        {
            logger.error("Kafka service name should not be null");
            return;
        }
        // kafka service data 与普通的不一致
        kafkaData = new ZNodeServiceDataImpl(kafkaHostPort, serviceGroupName, serviceName, -1, "kafkaUrl");
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
            closeKafkaProducer();
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
            catch (ServiceUnavailableException e)
            {
                logger.info("orchestrationService is stop");
                return;
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
        props.put("max.block.ms", 3000);
        
        producer = new KafkaProducer<Object, Object>(props);
    }
    
    private void closeKafkaProducer()
    {
        if (producer != null)
        {
            producer.close();
            producer = null;
        }
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
                logger.trace("Send msg to kafka timeout. Need to reconnect");
                closeKafkaProducer();
                try
                {
                    deleteKafkaRegZNode();
                    Thread.sleep(OrchestrationService.KAFKA_PRODUCER_TIMEOUT_SLEEP);
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
                catch (ServiceUnavailableException e)
                {
                    logger.info("orchestrationService is stop");
                    return;
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
                    Thread.sleep(OrchestrationService.KAFKA_PRODUCER_CHECK_INTERVAL);
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
                catch (ServiceUnavailableException e)
                {
                    logger.info("orchestrationService is stop");
                    return;
                }
            }
        }
    }
    
    private void createKafkaRegZNode() throws KeeperException, InterruptedException, ServiceUnavailableException
    {
        if (!orchestrationService.isRegZnodeExist(kafkaData.getServiceName()))
        {
            orchestrationService.createRegZnode(kafkaData.getServiceName(), kafkaData);
            logger.info("Create kafka znode: " + kafkaData.getServiceName());
        }
    }
    
    private void deleteKafkaRegZNode() throws KeeperException, InterruptedException, ServiceUnavailableException
    {
        if (orchestrationService.isRegZnodeExist(kafkaData.getServiceName()))
        {
            orchestrationService.deleteRegZnode(kafkaData.getServiceName());
            logger.info("Delete kafka znode: " + kafkaData.getServiceName());
        }
    }

}
