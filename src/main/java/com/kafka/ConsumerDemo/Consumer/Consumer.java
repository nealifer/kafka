package com.kafka.ConsumerDemo.Consumer;


import javafx.util.Duration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Created by njrussel on 3/2/20.
 */
public class Consumer {

    public static void main(String[] args) {


        new Consumer().run();

        /*
        //create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);


        // subscribe consumer to our topics(s)

        consumer.subscribe(Collections.singleton(topic));

        while(true){

           ConsumerRecords<String,String> records = consumer.poll(java.time.Duration.ofMillis(100)); //new kafka 2.0.0

            for (ConsumerRecord<String, String> record : records){
                logger.info("Key: " + record.key() + ", VAkue: " + record.value());
                logger.info("Partion:" + record.partition() + "Offset: "  + record.offset() );
            }
        }

    */


    }

    private Consumer(){}

    private void run(){

        Logger logger = LoggerFactory.getLogger(Consumer.class.getName());

        //groups

        String kafka = "10.0.0.147:9092";
        String kafka2 = "10.0.0.241:9092";
        String kafka3 = "10.0.0.25";

        String groupId = "logs";
        String topic = "serverlogs";

        CountDownLatch latch = new CountDownLatch(1);
        logger.info("Creating consumer thread");
        Runnable myConsumerThread = new ConsumerThread(kafka, kafka2, kafka3,groupId,topic,latch);

        //star the thread
        Thread myThread = new Thread(myConsumerThread);
        myThread.start();

        //shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(()-> {
            logger.info("Caight shutdown hook");
            ((ConsumerThread) myConsumerThread).shutdown();
        }
        ));

        try{
            latch.await();
        }catch (InterruptedException e){
            logger.error("Appligation got interuppted",e);
        } finally {
            logger.info("Application is closing");
        }





    }


    public class ConsumerThread implements Runnable{


        private CountDownLatch latch;
        private KafkaConsumer<String,String> consumer;
        private Logger logger = LoggerFactory.getLogger(Consumer.class.getName());


        public ConsumerThread(String first, String second, String third, String groupId, String topic,CountDownLatch latch){


            this.latch = latch;

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "first,second,third");
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, second);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList(topic));
        }
        @Override
        public void run() {
            try {
                while (true) {

                    ConsumerRecords<String, String> records = consumer.poll(java.time.Duration.ofMillis(100)); //new kafka 2.0.0

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", VAkue: " + record.value());
                        logger.info("Partion:" + record.partition() + "Offset: " + record.offset());
                    }
                }

            } catch (WakeupException e){
                logger.info("Recieved shutdown signal!");
            } finally {
                consumer.close();
                // tell our main code we're done with consumer
                latch.countDown();
            }
        }




        public void shutdown(){
            // the wakeup() method is a specital method to interrrupt consumer.poll
            // it will throw wakeup exception
            consumer.wakeup();

        }
    }



}
