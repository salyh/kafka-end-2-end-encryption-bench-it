package de.saly.kafka.crypto.benchmark;

import java.io.File;
import java.io.FileOutputStream;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.infra.Blackhole;

import de.saly.kafka.crypto.DecryptingDeserializer;
import de.saly.kafka.crypto.EncryptingSerializer;
import de.saly.kafka.crypto.SerdeCryptoBase;

@State(Scope.Benchmark)
public class SerDeBenchmark {
    
    private static String TOPIC = "topic";
    private byte[] testData1k = new byte[1024];
    private byte[] testData4k = new byte[4096];
    private byte[] testData8k = new byte[2*4096];
    private byte[] testData256k = new byte[256*1024];
    
    private byte[] testData1kCrypt;
    private byte[] testData4kCrypt;
    private byte[] testData8kCrypt;
    private byte[] testData256kCrypt;
    
    private File pubKey;
    private File privKey;
    private byte[] publicKey;
    private byte[] privateKey;
    
    private EncryptingSerializer<byte[]> serializer = new EncryptingSerializer<byte[]>();
    private DecryptingDeserializer<byte[]> deserializer = new DecryptingDeserializer<byte[]>();
    
    public SerDeBenchmark() {
        try {
            pubKey = File.createTempFile("kafka", "crypto");
            pubKey.deleteOnExit();
            privKey = File.createTempFile("kafka", "crypto");
            privKey.deleteOnExit();
            
            KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
            keyGen.initialize(2048);
            KeyPair pair = keyGen.genKeyPair();
            publicKey = pair.getPublic().getEncoded();
            privateKey = pair.getPrivate().getEncoded();
            
            try(FileOutputStream fout = new FileOutputStream(pubKey)) {
                fout.write(publicKey);
            }
            
            try(FileOutputStream fout = new FileOutputStream(privKey)) {
                fout.write(privateKey);
            }
                
            Map<String, Object> config = new HashMap<>();
            config.put(SerdeCryptoBase.CRYPTO_RSA_PRIVATEKEY_FILEPATH, privKey.getAbsolutePath());
            config.put(SerdeCryptoBase.CRYPTO_RSA_PUBLICKEY_FILEPATH, pubKey.getAbsolutePath());
            config.put(EncryptingSerializer.CRYPTO_VALUE_SERIALIZER, ByteArraySerializer.class.getName());
            config.put(DecryptingDeserializer.CRYPTO_VALUE_DESERIALIZER, ByteArrayDeserializer.class);
            serializer.configure(config, false);
            deserializer.configure(config, false);
            
            Random rand = new Random(); //no need for secure rand here
            
            rand.nextBytes(testData1k);
            rand.nextBytes(testData4k);
            rand.nextBytes(testData8k);
            rand.nextBytes(testData256k);
            
            testData1kCrypt = serializer.serialize(TOPIC, testData1k);
            testData4kCrypt = serializer.serialize(TOPIC, testData4k);
            testData8kCrypt = serializer.serialize(TOPIC, testData8k);
            testData256kCrypt = serializer.serialize(TOPIC, testData256k);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Benchmark
    @Threads(value=2)
    public void testSerializeEncryption1k(final Blackhole bh) {
        bh.consume(serializer.serialize(TOPIC, testData1k));
    }
    
    @Benchmark
    @Threads(value=2)
    public void testSerializeEncryption4k(final Blackhole bh) {
        bh.consume(serializer.serialize(TOPIC, testData4k));
    }
    
    @Benchmark
    @Threads(value=2)
    public void testSerializeEncryption8k(final Blackhole bh) {
        bh.consume(serializer.serialize(TOPIC, testData8k));
    }
    
    @Benchmark
    @Threads(value=2)
    public void testSerializeEncryption256k(final Blackhole bh) {
        bh.consume(serializer.serialize(TOPIC, testData256k));
    }
    
    @Benchmark
    @Threads(value=1)
    public void testDeserializeDecryption1k(final Blackhole bh) {
        bh.consume(deserializer.deserialize(TOPIC, testData1kCrypt));
    }
    
    @Benchmark
    @Threads(value=1)
    public void testDeserializeDecryption4k(final Blackhole bh) {
        bh.consume(deserializer.deserialize(TOPIC, testData4kCrypt));
    }
    
    @Benchmark
    @Threads(value=1)
    public void testDeserializeDecryption8k(final Blackhole bh) {
        bh.consume(deserializer.deserialize(TOPIC, testData8kCrypt));
    }
    
    @Benchmark
    @Threads(value=1)
    public void testDeserializeDecryption256k(final Blackhole bh) {
        bh.consume(deserializer.deserialize(TOPIC, testData256kCrypt));
    }
}
