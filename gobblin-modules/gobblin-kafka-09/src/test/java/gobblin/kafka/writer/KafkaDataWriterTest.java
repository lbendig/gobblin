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

package gobblin.kafka.writer;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import gobblin.kafka.KafkaTestBase;
import gobblin.kafka.schemareg.ConfigDrivenMd5SchemaRegistry;
import gobblin.kafka.schemareg.KafkaSchemaRegistryConfigurationKeys;
import gobblin.kafka.schemareg.SchemaRegistryException;
import gobblin.kafka.serialize.LiAvroDeserializer;
import gobblin.kafka.serialize.LiAvroSerializer;
import gobblin.test.TestUtils;
import gobblin.writer.WriteCallback;
import gobblin.writer.WriteResponse;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class KafkaDataWriterTest {


  private final KafkaTestBase _kafkaTestHelper;
  public KafkaDataWriterTest()
      throws InterruptedException, RuntimeException {
    _kafkaTestHelper = new KafkaTestBase();
  }

  @BeforeSuite
  public void beforeSuite() {
    log.warn("Process id = " + ManagementFactory.getRuntimeMXBean().getName());

    _kafkaTestHelper.startServers();
  }

  @AfterSuite
  public void afterSuite()
      throws IOException {
    try {
      _kafkaTestHelper.stopClients();
    }
    finally {
      _kafkaTestHelper.stopServers();
    }
  }

  @Test
  public void testStringSerialization()
      throws IOException, InterruptedException, ExecutionException {
    String topic = "testStringSerialization08";
    _kafkaTestHelper.provisionTopic(topic);
    Properties props = new Properties();
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_TOPIC, topic);
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX+"bootstrap.servers", "localhost:" + _kafkaTestHelper.getKafkaServerPort());
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX+"value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    KafkaDataWriter<String> kafka09DataWriter = new KafkaDataWriter<String>(props);
    String messageString = "foobar";
    WriteCallback callback = mock(WriteCallback.class);
    Future<WriteResponse> future;

    try {
      future = kafka09DataWriter.write(messageString, callback);
      kafka09DataWriter.flush();
      verify(callback, times(1)).onSuccess(isA(WriteResponse.class));
      verify(callback, never()).onFailure(isA(Exception.class));
      Assert.assertTrue(future.isDone(), "Future should be done");
      System.out.println(future.get().getStringResponse());
      byte[] message = _kafkaTestHelper.getIteratorForTopic(topic).next().message();
      String messageReceived = new String(message);
      Assert.assertEquals(messageReceived, messageString);
    }
    finally
    {
      kafka09DataWriter.close();
    }


  }

  @Test
  public void testBinarySerialization()
      throws IOException, InterruptedException {
    String topic = "testBinarySerialization08";
    _kafkaTestHelper.provisionTopic(topic);
    Properties props = new Properties();
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_TOPIC, topic);
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX+"bootstrap.servers", "localhost:" + _kafkaTestHelper.getKafkaServerPort());
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX+"value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    KafkaDataWriter<byte[]> kafka09DataWriter = new KafkaDataWriter<byte[]>(props);
    WriteCallback callback = mock(WriteCallback.class);
    byte[] messageBytes = TestUtils.generateRandomBytes();

    try {
      kafka09DataWriter.write(messageBytes, callback);
    }
    finally
    {
      kafka09DataWriter.close();
    }

    verify(callback, times(1)).onSuccess(isA(WriteResponse.class));
    verify(callback, never()).onFailure(isA(Exception.class));
    byte[] message = _kafkaTestHelper.getIteratorForTopic(topic).next().message();
    Assert.assertEquals(message, messageBytes);
  }

  @Test
  public void testAvroSerialization()
      throws IOException, InterruptedException, SchemaRegistryException {
    String topic = "testAvroSerialization08";
    _kafkaTestHelper.provisionTopic(topic);
    Properties props = new Properties();
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_TOPIC, topic);
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX + "bootstrap.servers",
        "localhost:" + _kafkaTestHelper.getKafkaServerPort());
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX + "value.serializer",
        LiAvroSerializer.class.getName());

    // set up mock schema registry

    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX
        + KafkaSchemaRegistryConfigurationKeys.KAFKA_SCHEMA_REGISTRY_CLASS,
        ConfigDrivenMd5SchemaRegistry.class.getCanonicalName());

    KafkaDataWriter<GenericRecord> kafka09DataWriter = new KafkaDataWriter<>(props);
    WriteCallback callback = mock(WriteCallback.class);

    GenericRecord record = TestUtils.generateRandomAvroRecord();
    try {
      kafka09DataWriter.write(record, callback);
    }
    finally
    {
      kafka09DataWriter.close();
    }

    verify(callback, times(1)).onSuccess(isA(WriteResponse.class));
    verify(callback, never()).onFailure(isA(Exception.class));

    byte[] message = _kafkaTestHelper.getIteratorForTopic(topic).next().message();
    ConfigDrivenMd5SchemaRegistry schemaReg = new ConfigDrivenMd5SchemaRegistry(topic, record.getSchema());
    LiAvroDeserializer deser = new LiAvroDeserializer(schemaReg);
    GenericRecord receivedRecord = deser.deserialize(topic, message);
    Assert.assertEquals(record.toString(), receivedRecord.toString());
  }

}
