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

package gobblin.source.extractor.extract.kafka;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.testng.Assert;
import org.testng.annotations.Test;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.base.Optional;
import com.typesafe.config.Config;

import gobblin.configuration.WorkUnitState;
import gobblin.kafka.client.GobblinKafkaConsumerClient;
import gobblin.kafka.client.GobblinKafkaConsumerClient.GobblinKafkaConsumerClientFactory;
import gobblin.kafka.client.KafkaConsumerClient;
import gobblin.metrics.kafka.KafkaSchemaRegistry;
import gobblin.source.extractor.WatermarkInterval;
import gobblin.source.extractor.extract.kafka.KafkaDeserializerTestUtils.MockWorkUnitState;
import gobblin.util.PropertiesUtils;

import avro.shaded.com.google.common.collect.Lists;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;


/**
 * Tests {@link gobblin.source.extractor.extract.kafka.KafkaDeserializerExtractor} using Kafka011 deserializers
 *
 */
@Test(groups = { "gobblin.source.extractor.extract.kafka" })
public class KafkaDeserializerExtractorTest extends KafkaDeserializerExtractorTestBase {

  private static final Optional<WatermarkInterval> DEFAULT_WM_INTERVAL =
      Optional.of(new WatermarkInterval(new MultiLongWatermark(Lists.newArrayList(0L)),
          new MultiLongWatermark(Lists.newArrayList(1L))));
  
  public static class MockKafkaConsumerClient<K, V> extends KafkaConsumerClient<K, V> {

    public MockKafkaConsumerClient(Config config, Optional<Consumer<K, V>> consumer) {
      super(config, consumer);
    }

    private static <K, V> MockConsumer<K, V> getMockConsumer() {
      MockConsumer<K, V> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
      mockConsumer.assign(Arrays.asList(new TopicPartition(TEST_TOPIC_NAME, 0)));

      Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
      beginningOffsets.put(new TopicPartition(TEST_TOPIC_NAME, 0), 0L);
      mockConsumer.updateBeginningOffsets(beginningOffsets);
      return mockConsumer;
    }

  }

  public static abstract class BaseFactory<K, V> implements GobblinKafkaConsumerClientFactory {
    @Override
    public GobblinKafkaConsumerClient create(Config config) {
      MockConsumer<K, V> mockConsumer = MockKafkaConsumerClient.getMockConsumer();
      mockConsumer.addRecord(produceRecord());
      return new MockKafkaConsumerClient<K, V>(config, Optional.of(mockConsumer));
    }

    public abstract ConsumerRecord<K, V> produceRecord();

  }

  public static class ByteArrayConsumerFactory extends BaseFactory<Object, byte[]> {
    @Override
    public ConsumerRecord<Object, byte[]> produceRecord() {
      String testString = "Hello World";
      return new ConsumerRecord<Object, byte[]>(TEST_TOPIC_NAME, 0, 0L, null,
          testString.getBytes(StandardCharsets.UTF_8));
    }
  }

  public static class ConfluentAvroConsumerFactory extends BaseFactory<Object, GenericRecord> {
    @Override
    public ConsumerRecord<Object, GenericRecord> produceRecord() {
      Schema schema = SchemaBuilder.record(TEST_RECORD_NAME)
          .namespace(TEST_NAMESPACE).fields()
          .name(TEST_FIELD_NAME).type().stringType().noDefault()
          .endRecord();
      
      GenericRecord testGenericRecord = new GenericRecordBuilder(schema).set(TEST_FIELD_NAME, "testValue").build();
      return new ConsumerRecord<Object, GenericRecord>(TEST_TOPIC_NAME, 0, 0L, null, testGenericRecord);
    }
  }

  public static class ConfluentJsonConsumerFactory extends BaseFactory<Object, KafkaRecord> {
    @Override
    public ConsumerRecord<Object, KafkaRecord> produceRecord() {
      return new ConsumerRecord<Object, KafkaRecord>(TEST_TOPIC_NAME, 0, 0L, null, new KafkaRecord("Hello World"));
    }
  }

  @Test
  public void testByteArrayDeserializer() throws Exception {

    WorkUnitState mockWorkUnitState = new MockWorkUnitState(DEFAULT_WM_INTERVAL)
        .withDeserializerName("BYTE_ARRAY")
        .with(KafkaSource.PARTITION_ID, 0)
        .with(KafkaSource.LEADER_ID, 0)
        .with(KafkaSource.LEADER_HOSTANDPORT, 0)
        .with(KafkaSource.GOBBLIN_KAFKA_CONSUMER_CLIENT_FACTORY_CLASS, ByteArrayConsumerFactory.class.getName());

    String testString = "Hello World";

    Deserializer<byte[]> kafkaDecoder = new ByteArrayDeserializer();
    kafkaDecoder.configure(PropertiesUtils.propsToStringKeyMap(mockWorkUnitState.getProperties()), false);

    KafkaDeserializerExtractor kafkaDecoderExtractor = KafkaDeserializerTestUtils
        .getMockDeserializerExtractor(mockWorkUnitState, new KafkaDeserializerWrapper(kafkaDecoder), Optional.absent());

    String received = new String((byte[]) kafkaDecoderExtractor.readRecordImpl(null));

    Assert.assertEquals(received, testString);
    kafkaDecoderExtractor.kafkaConsumerClient.close();

  }

  @Test
  public void testBuiltInStringDeserializerSetup() throws Exception {
    super.checkBuiltInStringDeserializerSetup();
  }

  @Test
  public void testBuiltInGsonDeserializerSetup() throws Exception {
    super.checkBuiltInGsonDeserializerSetup();
  }

  @Test
  public void testBuiltInConfluentAvroDeserializerSetup() throws Exception {
    super.checkBuiltInConfluentAvroDeserializerSetup();
  }

  public static class CustomDeserializer implements Deserializer<String> {

    public CustomDeserializer() {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public String deserialize(String topic, byte[] data) {
      return null;
    }

    @Override
    public void close() {
    }
  }

  @Test
  public void testCustomDeserializerSetup() throws Exception {

    WorkUnitState mockWorkUnitState = new MockWorkUnitState()
        .withDeserializerName(CustomDeserializer.class.getName())
        .withSchemaRegistryName(SimpleKafkaSchemaRegistry.class.getName());
    
    KafkaDeserializerExtractor kafkaDecoderExtractor = new KafkaDeserializerExtractor(mockWorkUnitState);

    Assert.assertEquals(kafkaDecoderExtractor.getKafkaDeserializer().getDeserializerClass(), CustomDeserializer.class);
    Assert.assertEquals(kafkaDecoderExtractor.getKafkaSchemaRegistry().getClass(), SimpleKafkaSchemaRegistry.class);
    kafkaDecoderExtractor.kafkaConsumerClient.close();
  }
  
  @Test
  public void testCustomDeserializerSetupError() throws Exception {

    WorkUnitState mockWorkUnitState = new MockWorkUnitState().withDeserializerName(CustomDeserializer.class.getName());

    try {
      KafkaDeserializerExtractor kafkaDecoderExtractor = new KafkaDeserializerExtractor(mockWorkUnitState);
      Assert.fail("KafkaDeserializerExtractor is not allowed to run without schema registry configuration");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(e.getMessage(), String.format("Unable to find %s for custom %s",
          KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_CLASS, KafkaDeserializerExtractor.KAFKA_DESERIALIZER_TYPE));
    }
  }

  @Test
  public void testConfluentAvroDeserializer() throws Exception {

    WorkUnitState mockWorkUnitState = new MockWorkUnitState(DEFAULT_WM_INTERVAL)
        .withDeserializerName("CONFLUENT_AVRO")
        .with(KafkaSource.PARTITION_ID, 0)
        .with(KafkaSource.LEADER_ID, 0)
        .with(KafkaSource.LEADER_HOSTANDPORT, 0)
        .with(ConfluentKafkaSchemaRegistry.CONFLUENT_SCHEMA_REGISTRY_URL, TEST_URL)
        .with(KafkaSource.GOBBLIN_KAFKA_CONSUMER_CLIENT_FACTORY_CLASS, ConfluentAvroConsumerFactory.class.getName());

    Schema schema = SchemaBuilder.record(TEST_RECORD_NAME)
        .namespace(TEST_NAMESPACE).fields()
        .name(TEST_FIELD_NAME).type().stringType().noDefault()
        .endRecord();
    GenericRecord testGenericRecord = new GenericRecordBuilder(schema).set(TEST_FIELD_NAME, "testValue").build();

    SchemaRegistryClient mockSchemaRegistryClient = mock(SchemaRegistryClient.class);
    when(mockSchemaRegistryClient.getBySubjectAndID(any(String.class), any(Integer.class))).thenReturn(schema);

    Deserializer<Object> kafkaDecoder = new KafkaAvroDeserializer(mockSchemaRegistryClient);
    kafkaDecoder.configure(PropertiesUtils.propsToStringKeyMap(mockWorkUnitState.getProperties()), false);

    KafkaDeserializerExtractor kafkaDecoderExtractor = KafkaDeserializerTestUtils
        .getMockDeserializerExtractor(mockWorkUnitState, new KafkaDeserializerWrapper(kafkaDecoder), Optional.absent());

    Assert.assertEquals((GenericRecord) kafkaDecoderExtractor.readRecordImpl(null), testGenericRecord);
    kafkaDecoderExtractor.kafkaConsumerClient.close();

  }

  @Test
  public void testConfluentAvroDeserializerForSchemaEvolution() throws Exception {

    WorkUnitState mockWorkUnitState = new MockWorkUnitState(DEFAULT_WM_INTERVAL)
        .withDeserializerName("CONFLUENT_AVRO")
        .with(KafkaSource.PARTITION_ID, 0)
        .with(KafkaSource.LEADER_ID, 0)
        .with(KafkaSource.LEADER_HOSTANDPORT, 0)
        .with(ConfluentKafkaSchemaRegistry.CONFLUENT_SCHEMA_REGISTRY_URL, TEST_URL)
        .with(KafkaSource.GOBBLIN_KAFKA_CONSUMER_CLIENT_FACTORY_CLASS, ConfluentAvroConsumerFactory.class.getName());

    Schema schemaV1 = SchemaBuilder.record(TEST_RECORD_NAME)
        .namespace(TEST_NAMESPACE).fields()
        .name(TEST_FIELD_NAME).type().stringType().noDefault()
        .endRecord();

    Schema schemaV2 = SchemaBuilder.record(TEST_RECORD_NAME)
        .namespace(TEST_NAMESPACE).fields()
        .name(TEST_FIELD_NAME).type().stringType().noDefault().optionalString(TEST_FIELD_NAME2)
        .endRecord();

    SchemaRegistryClient mockSchemaRegistryClient = mock(SchemaRegistryClient.class);
    when(mockSchemaRegistryClient.getBySubjectAndID(any(String.class), any(Integer.class))).thenReturn(schemaV1);

    KafkaSchemaRegistry<Integer, Schema> mockKafkaSchemaRegistry = mock(KafkaSchemaRegistry.class);
    when(mockKafkaSchemaRegistry.getLatestSchemaByTopic(TEST_TOPIC_NAME)).thenReturn(schemaV2);

    Deserializer<Object> kafkaDecoder = new KafkaAvroDeserializer(mockSchemaRegistryClient);
    kafkaDecoder.configure(PropertiesUtils.propsToStringKeyMap(mockWorkUnitState.getProperties()), false);

    KafkaDeserializerExtractor kafkaDecoderExtractor = KafkaDeserializerTestUtils.getMockDeserializerExtractor(
        mockWorkUnitState, new KafkaDeserializerWrapper(kafkaDecoder), Optional.of(mockKafkaSchemaRegistry));

    GenericRecord retrieved = (GenericRecord) kafkaDecoderExtractor.readRecordImpl(null);
    Assert.assertEquals(retrieved.toString(), "{\"testField\": \"testValue\", \"testField2\": null}");

    kafkaDecoderExtractor.kafkaConsumerClient.close();

  }

  @Test
  public void testConfluentJsonDeserializer() throws Exception {

    WorkUnitState mockWorkUnitState = new MockWorkUnitState(DEFAULT_WM_INTERVAL)
        .withDeserializerName("CONFLUENT_JSON")
        .with("json.value.type", KafkaRecord.class.getName())
        .with(KafkaSource.GOBBLIN_KAFKA_CONSUMER_CLIENT_FACTORY_CLASS, ConfluentJsonConsumerFactory.class.getName());

    KafkaRecord testKafkaRecord = new KafkaRecord("Hello World");

    Deserializer<Object> kafkaDecoder = new KafkaJsonDeserializer<>();
    kafkaDecoder.configure(PropertiesUtils.propsToStringKeyMap(mockWorkUnitState.getProperties()), false);

    KafkaDeserializerExtractor kafkaDecoderExtractor = KafkaDeserializerTestUtils
        .getMockDeserializerExtractor(mockWorkUnitState, new KafkaDeserializerWrapper(kafkaDecoder), Optional.absent());

    KafkaRecord retrieved = (KafkaRecord) kafkaDecoderExtractor.readRecordImpl(null);
    Assert.assertEquals(retrieved, testKafkaRecord);

    kafkaDecoderExtractor.kafkaConsumerClient.close();
  }

}
