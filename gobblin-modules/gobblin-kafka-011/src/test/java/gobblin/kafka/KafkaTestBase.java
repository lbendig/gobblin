/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gobblin.kafka;


import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Time;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.typesafe.config.ConfigFactory;

import gobblin.configuration.ConfigurationKeys;
import gobblin.kafka.client.GobblinKafkaConsumerClient;
import gobblin.kafka.client.KafkaConsumerClient;
import gobblin.kafka.client.KafkaConsumerClient.Kafka011ConsumerRecord;
import gobblin.kafka.writer.KafkaWriterConfigurationKeys;
import gobblin.metrics.kafka.KafkaSchemaRegistry;
import gobblin.kafka.client.KafkaConsumerRecord;
import gobblin.source.extractor.extract.kafka.KafkaDeserializerExtractor;
import gobblin.source.extractor.extract.kafka.KafkaDeserializers;
import gobblin.source.extractor.extract.kafka.KafkaPartition;
import gobblin.test.TestUtils;

import kafka.admin.AdminUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import static scala.collection.JavaConversions.asScalaBuffer;


/**
 * A private class for starting a suite of servers for Kafka
 * Calls to start and shutdown are reference counted, so that the suite is started and shutdown in pairs.
 * A suite of servers (Zk, Kafka etc) will be started just once per process
 */
@Slf4j
@Getter
enum KafkaServerSuite {
  INSTANCE;

  @Getter(AccessLevel.NONE)
  private int brokerId = 0;

  @Getter(AccessLevel.NONE)
  private EmbeddedZookeeper zkServer;

  @Getter(AccessLevel.NONE)
  private final AtomicInteger numStarted;

  private ZkClient zkClient;
  private KafkaServer kafkaServer;
  private final int kafkaServerPort;
  private String zkConnectString;

  private KafkaServerSuite() {
    this.kafkaServerPort = TestUtils.findFreePort();
    this.zkConnectString = "UNINITIALIZED_HOST_PORT";
    this.numStarted = new AtomicInteger(0);
  }

  void start() throws RuntimeException {
    if (this.numStarted.incrementAndGet() == 1) {
      log.warn("Starting up Kafka server suite. Zk at " + zkConnectString + "; Kafka server at " + this.kafkaServerPort);
      this.zkServer = new EmbeddedZookeeper();
      this.zkConnectString = "127.0.0.1:" + this.zkServer.port();
      this.zkClient = ZkUtils.createZkClient(this.zkConnectString, 30000, 30000);
      Properties props = kafka.utils.TestUtils.createBrokerConfig(this.brokerId, this.zkConnectString,
          kafka.utils.TestUtils.createBrokerConfig$default$3(), kafka.utils.TestUtils.createBrokerConfig$default$4(),
          this.kafkaServerPort, kafka.utils.TestUtils.createBrokerConfig$default$6(),
          kafka.utils.TestUtils.createBrokerConfig$default$7(), kafka.utils.TestUtils.createBrokerConfig$default$8(),
          kafka.utils.TestUtils.createBrokerConfig$default$9(), kafka.utils.TestUtils.createBrokerConfig$default$10(),
          kafka.utils.TestUtils.createBrokerConfig$default$11(), kafka.utils.TestUtils.createBrokerConfig$default$12(),
          kafka.utils.TestUtils.createBrokerConfig$default$13(), kafka.utils.TestUtils.createBrokerConfig$default$14(),
          kafka.utils.TestUtils.createBrokerConfig$default$15(), kafka.utils.TestUtils.createBrokerConfig$default$16());
      KafkaConfig config = new KafkaConfig(props);
      this.kafkaServer = kafka.utils.TestUtils.createServer(config, Time.SYSTEM);
    } else {
      log.info("Kafka server suite already started... continuing");
    }
  }

  void shutdown() {
    if (this.numStarted.decrementAndGet() == 0) {
      log.info("Shutting down Kafka server suite");
      this.kafkaServer.shutdown();
      this.zkClient.close();
      this.zkServer.shutdown();
    } else {
      log.info("Kafka server suite still in use ... not shutting down yet");
    }
  }
}

class KafkaConsumerSuite {

  private final GobblinKafkaConsumerClient consumerClient;
  private final String topic;

  KafkaConsumerSuite(String zkConnectString, String topic, Properties consumerProps) {
    Properties props = new Properties();
    props.putAll(consumerProps);
    props.putIfAbsent("key.deserializer", ByteArrayDeserializer.class.getName());
    props.putIfAbsent("value.deserializer", ByteArrayDeserializer.class.getName());
    props.put("kafka.brokers", "localhost:" + KafkaServerSuite.INSTANCE.getKafkaServerPort());
    props.put("zookeeper.connect", zkConnectString);
    props.put("group.id", topic + "-" + System.nanoTime());
    props.put("zookeeper.session.timeout.ms", "10000");
    props.put("zookeeper.sync.time.ms", "10000");
    props.put("auto.commit.interval.ms", "10000");
    props.put("consumer.timeout.ms", "10000");
    props.put("enable.auto.commit", "false");
    props.put("auto.offset.reset", "earliest");

    this.topic = topic;
    this.consumerClient = new KafkaConsumerClient.Factory().create(ConfigFactory.parseProperties(props));
  }

  void shutdown() throws IOException {
    this.consumerClient.close();
  }

  public <K, V> Iterator<Kafka011ConsumerRecord<K, V>> getIterator() {
    KafkaPartition partition = new KafkaPartition.Builder().withId(0).withTopicName(this.topic).build();
    Iterator<Kafka011ConsumerRecord<K, V>> it = Iterators.transform(this.consumerClient.consume(partition, 0l, 100l),
        new Function<KafkaConsumerRecord, Kafka011ConsumerRecord<K, V>>() {
          @Override
          public Kafka011ConsumerRecord<K, V> apply(KafkaConsumerRecord input) {
            return (Kafka011ConsumerRecord<K, V>) input;
          }
        });
    return it;
  }
  
}

/**
 * A Helper class for testing against Kafka
 * A suite of servers (Zk, Kafka etc) will be started just once per process
 * Consumer and iterator will be created per instantiation and is one instance per topic.
 * @param <K>
 */
public class KafkaTestBase implements Closeable {

  public static final Function<String, String> prodPrefix =
      s -> KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX + s;

  private final KafkaServerSuite kafkaServerSuite;
  private final Map<String, KafkaConsumerSuite> topicConsumerMap;

  public KafkaTestBase() throws InterruptedException, RuntimeException {

    this.kafkaServerSuite = KafkaServerSuite.INSTANCE;
    this.topicConsumerMap = new HashMap<>();
  }

  public synchronized void startServers() {
    kafkaServerSuite.start();
  }

  public void stopServers() {
    kafkaServerSuite.shutdown();
  }

  public void start() {
    startServers();
  }

  public void stopClients() throws IOException {
    for (Map.Entry<String, KafkaConsumerSuite> consumerSuiteEntry : this.topicConsumerMap.entrySet()) {
      consumerSuiteEntry.getValue().shutdown();
      AdminUtils.deleteTopic(ZkUtils.apply(this.kafkaServerSuite.getZkClient(), false), consumerSuiteEntry.getKey());
    }
  }

  @Override
  public void close() throws IOException {
    stopClients();
    stopServers();
  }

  public void provisionTopic(String topic) {
    AdminUtils.createTopic(ZkUtils.apply(this.kafkaServerSuite.getZkClient(), false), topic, 1, 1,
        kafka.admin.AdminUtils.createTopic$default$5(), kafka.admin.AdminUtils.createTopic$default$6());
  }

  public <K,V>Iterator<Kafka011ConsumerRecord<K, V>> getIterator(String topic, Properties consumerProps) {
    kafka.utils.TestUtils.waitUntilMetadataIsPropagated(
        asScalaBuffer(Lists.newArrayList(this.kafkaServerSuite.getKafkaServer())), topic, 0, 5000);
    KafkaConsumerSuite consumerSuite =
        new KafkaConsumerSuite(this.kafkaServerSuite.getZkConnectString(), topic, consumerProps);
    return consumerSuite.getIterator();
  }

  public int getKafkaServerPort() {
    return this.kafkaServerSuite.getKafkaServerPort();
  }
  
  public Properties getProducerProps(String keySerializer, String valueSerializer) {
    Properties props = new Properties();
    props.setProperty(prodPrefix.apply("bootstrap.servers"), "localhost:" + getKafkaServerPort());
    props.setProperty(prodPrefix.apply("key.serializer"), keySerializer);
    props.setProperty(prodPrefix.apply("value.serializer"), valueSerializer);
    return props;
  }
  
  public Properties getConsumerProps(KafkaDeserializers deserializer) {
    return getConsumerProps(deserializer.getDeserializerClass().getName(),
        deserializer.getSchemaRegistryClass().getName());
  }

  public Properties getConsumerProps(String valueDeserializer, String schemaRegistry) {
    Properties props = new Properties();
    props.setProperty(KafkaDeserializerExtractor.KAFKA_DESERIALIZER_TYPE, valueDeserializer);
    props.setProperty(KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_CLASS, schemaRegistry);
    props.setProperty(KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_URL, "testUrl");
    props.put(ConfigurationKeys.JOB_ID_KEY, "testJob");
    return props;
  }
  
}
