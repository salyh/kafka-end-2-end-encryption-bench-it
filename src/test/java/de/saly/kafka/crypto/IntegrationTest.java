package de.saly.kafka.crypto;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.Test;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.Time;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;

public class IntegrationTest {

    private final static String TOPIC = "cryptedTestTopic";
    private final File pubKey;
    private final File privKey;
    private final byte[] publicKey;
    private final byte[] privateKey;
    
    private static final String ZKHOST = "127.0.0.1";
    private static final String BROKERHOST = "127.0.0.1";
    private static final int BROKERPORT = 9092;

    public IntegrationTest() throws Exception {
        pubKey = File.createTempFile("kafka", "crypto");
        pubKey.deleteOnExit();
        privKey = File.createTempFile("kafka", "crypto");
        privKey.deleteOnExit();

        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        keyGen.initialize(2048);
        KeyPair pair = keyGen.genKeyPair();
        publicKey = pair.getPublic().getEncoded();
        privateKey = pair.getPrivate().getEncoded();

        //System.out.println("private key format: "+pair.getPrivate().getFormat()); // PKCS#8
        //System.out.println("public key format: "+pair.getPublic().getFormat()); // X.509

        FileOutputStream fout = new FileOutputStream(pubKey);
        fout.write(publicKey);
        fout.close();

        fout = new FileOutputStream(privKey);
        fout.write(privateKey);
        fout.close();
    }


    @Test
    public void testInteg1() throws Exception {
        
        EmbeddedZookeeper zkServer = new EmbeddedZookeeper();
        String zkConnect = ZKHOST + ":" + zkServer.port();
        ZkClient zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
        ZkUtils zkUtils = ZkUtils.apply(zkClient, false);
        
        // setup Broker
        Properties brokerProps = new Properties();
        brokerProps.setProperty("zookeeper.connect", zkConnect);
        brokerProps.setProperty("broker.id", "0");
        final Path tmp = Files.createTempDirectory("kafka-");
        
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
              try {
                  Files.walkFileTree(tmp, new SimpleFileVisitor<Path>() {
                      @Override
                      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                              Files.delete(file);
                              return FileVisitResult.CONTINUE;
                      }

                      @Override
                      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                              Files.delete(dir);
                              return FileVisitResult.CONTINUE;
                      }

              });
              System.out.println("Temprary data under "+tmp.toAbsolutePath()+" deleted");
            } catch (IOException e) {
                e.printStackTrace();
            }
            }
          });
        
        
        brokerProps.setProperty("log.dirs", tmp.toAbsolutePath().toString());
        brokerProps.setProperty("listeners", "PLAINTEXT://" + BROKERHOST +":" + BROKERPORT);
        KafkaConfig config = new KafkaConfig(brokerProps);
        Time mock = new MockTime();
        KafkaServer kafkaServer = TestUtils.createServer(config, mock);

        try {
            // create topic
            AdminUtils.createTopic(zkUtils, TOPIC, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
            
            Properties producerProps = new Properties();
            producerProps.setProperty("bootstrap.servers", BROKERHOST + ":" + BROKERPORT);
            producerProps.put("enable.auto.commit", "true");
            producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producerProps.put("value.serializer", "de.saly.kafka.crypto.EncryptingSerializer");
            producerProps.put("crypto.wrapped_serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producerProps.put("crypto.rsa.publickey.filepath", pubKey.getAbsolutePath());

            try(Producer<String, String> producer = new KafkaProducer<String, String>(producerProps)) {
                for(int i = 0; i < 1000; i++) {
                    producer.send(new ProducerRecord<String, String>(TOPIC, Integer.toString(i), "test:"+Integer.toString(i)));
                }
            }
            
            Properties consumerProps = new Properties();
            consumerProps.setProperty("bootstrap.servers", BROKERHOST + ":" + BROKERPORT);
            consumerProps.put("group.id", "test");
            consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumerProps.put("value.deserializer", DecryptingDeserializer.class);
            consumerProps.put("crypto.wrapped_deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumerProps.put("crypto.rsa.privatekey.filepath", privKey.getAbsolutePath());
            consumerProps.put("auto.offset.reset", "earliest");
            
            try(KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProps)) {
                consumer.subscribe(Arrays.asList(TOPIC));
                
                int i =0;
                ConsumerRecords<String, String> records = consumer.poll(10000);
                
                Assert.assertEquals(1000, records.count());
                
                for (ConsumerRecord<String, String> record : records) {
                    assertEquals(String.valueOf(i), record.key());
                    assertEquals("test:"+Integer.toString(i++), record.value());
                }
            }
        } finally {
            kafkaServer.shutdown();
            kafkaServer.awaitShutdown();
            zkServer.shutdown();
        }
    }
    
    /*@Test
    public void testInteg2() throws Exception {
        
        final int proxyPort = 10991;
        //EncDecProxy.start(proxyPort, BROKERHOST, BROKERPORT);

        EmbeddedZookeeper zkServer = new EmbeddedZookeeper();
        String zkConnect = ZKHOST + ":" + zkServer.port();
        ZkClient zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
        ZkUtils zkUtils = ZkUtils.apply(zkClient, false);
        System.out.println(zkConnect);
        // setup Broker
        Properties brokerProps = new Properties();
        brokerProps.setProperty("zookeeper.connect", zkConnect);
        brokerProps.setProperty("broker.id", "0");
        brokerProps.setProperty("log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString());
        brokerProps.setProperty("listeners", "PLAINTEXT://" + BROKERHOST +":" + BROKERPORT);
        brokerProps.setProperty("advertised.listeners", "PLAINTEXT://" + BROKERHOST +":" + proxyPort);
        KafkaConfig config = new KafkaConfig(brokerProps);
        Time mock = new MockTime();
        KafkaServer kafkaServer = TestUtils.createServer(config, mock);

        // create topic
        AdminUtils.createTopic(zkUtils, TOPIC, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
        
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", BROKERHOST + ":" + BROKERPORT);
        producerProps.put("enable.auto.commit", "true");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(producerProps);
        System.out.println("start send");
        for(int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>(TOPIC, "keykeykeykeykeykeykeykeykeykeykey:"+Integer.toString(i), "datadatadatadatadatadatadatadatadatadatadata:"+Integer.toString(i)));
        }
        
        producer.close();
        System.out.println("end send");
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", BROKERHOST + ":" + BROKERPORT);
        consumerProps.put("group.id", "test");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "de.saly.kafka.crypto.DecryptingDeserializer");
        consumerProps.put("crypto.wrapped_deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("crypto.rsa.privatekey.filepath", privKey.getAbsolutePath());
        consumerProps.put("auto.offset.reset", "earliest");
        System.out.println("start consume");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProps);
        consumer.subscribe(Arrays.asList(TOPIC));
        
        int i =0;
        ConsumerRecords<String, String> records = consumer.poll(10000);
        
        Assert.assertEquals(10, records.count());
        
        for (ConsumerRecord<String, String> record : records) {
            assertEquals("keykeykeykeykeykeykeykeykeykeykey:"+String.valueOf(i), record.key());
            assertEquals("datadatadatadatadatadatadatadatadatadatadata:"+Integer.toString(i++), record.value());
        }
        System.out.println("end consume");
    }
    
    
    @Test
    public void testInteg3() throws Exception {
        
        final int proxyPort = 10991;
        EncDecProxy.start(proxyPort, BROKERHOST, BROKERPORT);

        EmbeddedZookeeper zkServer = new EmbeddedZookeeper();
        String zkConnect = ZKHOST + ":" + zkServer.port();
        ZkClient zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
        ZkUtils zkUtils = ZkUtils.apply(zkClient, false);

        // setup Broker
        Properties brokerProps = new Properties();
        brokerProps.setProperty("zookeeper.connect", zkConnect);
        brokerProps.setProperty("broker.id", "0");
        brokerProps.setProperty("log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString());
        brokerProps.setProperty("listeners", "PLAINTEXT://" + BROKERHOST +":" + BROKERPORT);
        KafkaConfig config = new KafkaConfig(brokerProps);
        Time mock = new MockTime();
        KafkaServer kafkaServer = TestUtils.createServer(config, mock);

        // create topic
        AdminUtils.createTopic(zkUtils, TOPIC, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
        
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", BROKERHOST + ":" + BROKERPORT);
        producerProps.put("enable.auto.commit", "true");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "de.saly.kafka.crypto.EncryptingSerializer");
        producerProps.put("crypto.wrapped_serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("crypto.rsa.publickey.filepath", pubKey.getAbsolutePath());


        Producer<String, String> producer = new KafkaProducer<String, String>(producerProps);
        System.out.println("start send");
        for(int i = 0; i < 1000; i++) {
            producer.send(new ProducerRecord<String, String>(TOPIC, "key:"+Integer.toString(i), "data:"+Integer.toString(i)));
        }
        
        producer.close();
        System.out.println("end send");
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", BROKERHOST + ":" + proxyPort);
        consumerProps.put("group.id", "test");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("auto.offset.reset", "earliest");
        System.out.println("start consume");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProps);
        consumer.subscribe(Arrays.asList(TOPIC));
        
        int i =0;
        ConsumerRecords<String, String> records = consumer.poll(10000);
        
        Assert.assertEquals(1000, records.count());
        
        for (ConsumerRecord<String, String> record : records) {
            assertEquals("key:"+String.valueOf(i), record.key());
            assertEquals("data:"+Integer.toString(i++), record.value());
        }
        System.out.println("end consume");
    }*/
}
