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
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import gobblin.kafka.KafkaTestBase;
import gobblin.kafka.client.KafkaConsumerClient.Kafka011ConsumerRecord;
import gobblin.kafka.schemareg.SchemaRegistryException;
import gobblin.source.extractor.extract.kafka.ConfluentKafkaSchemaRegistry;
import gobblin.source.extractor.extract.kafka.KafkaDeserializers;
import gobblin.test.TestUtils;
import gobblin.writer.WriteCallback;
import gobblin.writer.WriteResponse;
import static gobblin.kafka.KafkaTestBase.prodPrefix;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class KafkaDataWriterTest {

  private final KafkaTestBase kafkaTestHelper;
  
  public KafkaDataWriterTest() throws InterruptedException, RuntimeException {
    this.kafkaTestHelper = new KafkaTestBase();
  }

  @BeforeSuite
  public void beforeSuite() {
    log.warn("Process id = " + ManagementFactory.getRuntimeMXBean().getName());
    this.kafkaTestHelper.startServers();
  }

  @AfterSuite
  public void afterSuite() throws IOException {
    this.kafkaTestHelper.close();
  }

  @Test
  public void testStringSerialization() throws IOException {
    String topicName = "testStringSerialization";
    String record = "foobar";

    Properties producerProps =
        this.kafkaTestHelper.getProducerProps(StringSerializer.class.getName(), StringSerializer.class.getName());
    writeRecord(producerProps, topicName, record);

    Properties consumerProps = this.kafkaTestHelper.getConsumerProps(KafkaDeserializers.STRING);
    Iterator<Kafka011ConsumerRecord<String, String>> it = this.kafkaTestHelper.getIterator(topicName, consumerProps);
    String receivedRecord = it.next().getValue();
    Assert.assertEquals(record, receivedRecord);
  }

  @Test
  public void testBinarySerialization() throws IOException {
    String topicName = "testBinarySerialization";
    byte[] record = TestUtils.generateRandomBytes();

    Properties producerProps =
        this.kafkaTestHelper.getProducerProps(StringSerializer.class.getName(), ByteArraySerializer.class.getName());
    writeRecord(producerProps, topicName, record);

    Properties consumerProps = this.kafkaTestHelper.getConsumerProps(KafkaDeserializers.BYTE_ARRAY);
    Iterator<Kafka011ConsumerRecord<String, byte[]>> it = this.kafkaTestHelper.getIterator(topicName, consumerProps);
    byte[] receivedRecord = it.next().getValue();
    Assert.assertEquals(record, receivedRecord);
  }

  public static class MockKafkaAvroSerializer extends KafkaAvroSerializer {
    public MockKafkaAvroSerializer() {
      super();
      this.schemaRegistry = new MockSchemaRegistryClient();
    }
  }

  public static class MockKafkaAvroDeserializer extends KafkaAvroDeserializer {
    public MockKafkaAvroDeserializer() {
      super();
      try {
        SchemaRegistryClient mockSchemaRegistryClient = mock(SchemaRegistryClient.class);
        when(mockSchemaRegistryClient.getBySubjectAndID(any(String.class), any(Integer.class)))
            .thenReturn(TestUtils.generateRandomAvroRecordSchema());
        this.schemaRegistry = mockSchemaRegistryClient;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  public void testAvroSerialization() throws IOException, SchemaRegistryException, RestClientException {
    String topicName = "testAvroSerialization";
    GenericRecord record = TestUtils.generateRandomAvroRecord();

    Properties producerProps = this.kafkaTestHelper.getProducerProps(StringSerializer.class.getName(),
        MockKafkaAvroSerializer.class.getName());
    producerProps.setProperty(prodPrefix.apply(ConfluentKafkaSchemaRegistry.CONFLUENT_SCHEMA_REGISTRY_URL), "testUrl");

    writeRecord(producerProps, topicName, record);

    Properties consumerProps = this.kafkaTestHelper.getConsumerProps(MockKafkaAvroDeserializer.class.getName(),
        ConfluentKafkaSchemaRegistry.class.getName());

    Iterator<Kafka011ConsumerRecord<String, GenericRecord>> it =
        this.kafkaTestHelper.getIterator(topicName, consumerProps);
    GenericRecord receivedRecord = it.next().getValue();
    Assert.assertEquals(record.toString(), receivedRecord.toString());
  }

  private <T> void writeRecord(Properties producerProps, String topicName, T record) throws IOException {
    producerProps.setProperty("writer.kafka.topic", topicName);
    try (KafkaDataWriter<T> kafkaDataWriter = new KafkaDataWriter<>(producerProps);) {
      this.kafkaTestHelper.provisionTopic(topicName);
      WriteCallback callback = mock(WriteCallback.class);
      Future<WriteResponse> future = kafkaDataWriter.write(record, callback);
      kafkaDataWriter.flush();
      verify(callback, times(1)).onSuccess(isA(WriteResponse.class));
      verify(callback, never()).onFailure(isA(Exception.class));
      Assert.assertTrue(future.isDone(), "Future should be done");
    }
  }
  
}
