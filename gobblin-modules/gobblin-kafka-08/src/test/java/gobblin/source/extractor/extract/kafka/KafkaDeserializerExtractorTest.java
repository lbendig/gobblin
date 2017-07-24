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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.testng.Assert;
import org.testng.annotations.Test;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.base.Optional;

import gobblin.configuration.WorkUnitState;
import gobblin.kafka.client.ByteArrayBasedKafkaRecord;
import gobblin.kafka.client.KafkaConsumerClient;
import gobblin.metrics.kafka.KafkaSchemaRegistry;
import gobblin.source.extractor.extract.kafka.KafkaDeserializerTestUtils.MockWorkUnitState;
import gobblin.util.PropertiesUtils;
import static gobblin.source.extractor.extract.kafka.KafkaDeserializerTestUtils.getMockDeserializerExtractor;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;

import kafka.message.Message;
import kafka.message.MessageAndOffset;


/**
 * Tests {@link gobblin.source.extractor.extract.kafka.KafkaDeserializerExtractor} using Kafka08 deserializers
 *
 */
@Test(groups = { "gobblin.source.extractor.extract.kafka" })
public class KafkaDeserializerExtractorTest extends KafkaDeserializerExtractorTestBase {

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
  
  @Test
  public void testByteArrayDeserializer() throws IOException {
    WorkUnitState mockWorkUnitState = new MockWorkUnitState().withDeserializerName("BYTE_ARRAY");

    String testString = "Hello World";
    ByteBuffer testStringByteBuffer = ByteBuffer.wrap(testString.getBytes(StandardCharsets.UTF_8));
    
    Deserializer<byte[]> kafkaDecoder = new ByteArrayDeserializer();
    kafkaDecoder.configure(PropertiesUtils.propsToStringKeyMap(mockWorkUnitState.getProperties()), false);

    KafkaDeserializerExtractor kafkaDecoderExtractor =
        KafkaDeserializerTestUtils.getMockDeserializerExtractor(mockWorkUnitState, new KafkaDeserializerWrapper(kafkaDecoder), Optional.absent());
    
    ByteArrayBasedKafkaRecord mockMessageAndOffset = getMockMessageAndOffset(testStringByteBuffer);
    Assert.assertEquals(new String((byte[])kafkaDecoderExtractor.decodeRecord(mockMessageAndOffset)), testString);
    kafkaDecoderExtractor.kafkaConsumerClient.close();
  }

  @Test
  public void testConfluentAvroDeserializer() throws Exception {
    WorkUnitState mockWorkUnitState = new MockWorkUnitState()
        .withDeserializerName(KafkaDeserializers.CONFLUENT_AVRO.toString())
        .with(ConfluentKafkaSchemaRegistry.CONFLUENT_SCHEMA_REGISTRY_URL, TEST_URL);

    Schema schema = SchemaBuilder.record(TEST_RECORD_NAME)
        .namespace(TEST_NAMESPACE).fields()
        .name(TEST_FIELD_NAME).type().stringType().noDefault()
        .endRecord();
    
    GenericRecord testGenericRecord = new GenericRecordBuilder(schema).set(TEST_FIELD_NAME, "testValue").build();

    SchemaRegistryClient mockSchemaRegistryClient = mock(SchemaRegistryClient.class);
    when(mockSchemaRegistryClient.getBySubjectAndID(any(String.class), any(Integer.class))).thenReturn(schema);

    Serializer<Object> kafkaEncoder = new KafkaAvroSerializer(mockSchemaRegistryClient);
    Deserializer<Object> kafkaDecoder = new KafkaAvroDeserializer(mockSchemaRegistryClient);

    ByteBuffer testGenericRecordByteBuffer =
        ByteBuffer.wrap(kafkaEncoder.serialize(TEST_TOPIC_NAME, testGenericRecord));

    KafkaDeserializerExtractor kafkaDecoderExtractor =
        KafkaDeserializerTestUtils.getMockDeserializerExtractor(mockWorkUnitState, new KafkaDeserializerWrapper(kafkaDecoder), Optional.absent());
    ByteArrayBasedKafkaRecord mockMessageAndOffset = getMockMessageAndOffset(testGenericRecordByteBuffer);
   
    Assert.assertEquals(kafkaDecoderExtractor.decodeRecord(mockMessageAndOffset), testGenericRecord);
    kafkaDecoderExtractor.kafkaConsumerClient.close();
   
  }
  
  @Test
  public void testConfluentAvroDeserializerForSchemaEvolution() throws Exception {
    WorkUnitState mockWorkUnitState = new MockWorkUnitState()
        .withDeserializerName(KafkaDeserializers.CONFLUENT_AVRO.name())
        .with(ConfluentKafkaSchemaRegistry.CONFLUENT_SCHEMA_REGISTRY_URL, TEST_URL);
    
    Schema schemaV1 = SchemaBuilder.record(TEST_RECORD_NAME)
        .namespace(TEST_NAMESPACE)
        .fields()
        .name(TEST_FIELD_NAME).type().stringType().noDefault()
        .endRecord();

    Schema schemaV2 = SchemaBuilder.record(TEST_RECORD_NAME)
        .namespace(TEST_NAMESPACE)
        .fields()
        .name(TEST_FIELD_NAME).type().stringType().noDefault()
        .optionalString(TEST_FIELD_NAME2)
        .endRecord();

    GenericRecord testGenericRecord = new GenericRecordBuilder(schemaV1).set(TEST_FIELD_NAME, "testValue").build();

    SchemaRegistryClient mockSchemaRegistryClient = mock(SchemaRegistryClient.class);
    when(mockSchemaRegistryClient.getBySubjectAndID(any(String.class), any(Integer.class))).thenReturn(schemaV1);

    Serializer<Object> kafkaEncoder = new KafkaAvroSerializer(mockSchemaRegistryClient);
    Deserializer<Object> kafkaDecoder = new KafkaAvroDeserializer(mockSchemaRegistryClient);

    ByteBuffer testGenericRecordByteBuffer =
        ByteBuffer.wrap(kafkaEncoder.serialize(TEST_TOPIC_NAME, testGenericRecord));

    KafkaSchemaRegistry<Integer, Schema> mockKafkaSchemaRegistry = mock(KafkaSchemaRegistry.class);
    when(mockKafkaSchemaRegistry.getLatestSchemaByTopic(TEST_TOPIC_NAME)).thenReturn(schemaV2);

    KafkaDeserializerExtractor kafkaDecoderExtractor = getMockDeserializerExtractor(mockWorkUnitState,
        new KafkaDeserializerWrapper(kafkaDecoder), Optional.of(mockKafkaSchemaRegistry));

    ByteArrayBasedKafkaRecord mockMessageAndOffset = getMockMessageAndOffset(testGenericRecordByteBuffer);

    GenericRecord received = (GenericRecord) kafkaDecoderExtractor.decodeRecord(mockMessageAndOffset);
    Assert.assertEquals(received.toString(), "{\"testField\": \"testValue\", \"testField2\": null}");
    kafkaDecoderExtractor.kafkaConsumerClient.close();
  }

  @Test
  public void testConfluentJsonDeserializer() throws Exception {
    WorkUnitState mockWorkUnitState = new MockWorkUnitState()
        .withDeserializerName("CONFLUENT_JSON")
        .with("json.value.type", KafkaRecord.class.getName());

    KafkaRecord testKafkaRecord = new KafkaRecord("Hello World");

    Serializer<Object> kafkaEncoder = new KafkaJsonSerializer<>();
    kafkaEncoder.configure(PropertiesUtils.propsToStringKeyMap(mockWorkUnitState.getProperties()), false);

    Deserializer<Object> kafkaDecoder = new KafkaJsonDeserializer<>();
    kafkaDecoder.configure(PropertiesUtils.propsToStringKeyMap(mockWorkUnitState.getProperties()), false);

    ByteBuffer testKafkaRecordByteBuffer = ByteBuffer.wrap(kafkaEncoder.serialize(TEST_TOPIC_NAME, testKafkaRecord));

    KafkaDeserializerExtractor kafkaDecoderExtractor =
        KafkaDeserializerTestUtils.getMockDeserializerExtractor(mockWorkUnitState, new KafkaDeserializerWrapper(kafkaDecoder), Optional.absent());
    
    ByteArrayBasedKafkaRecord mockMessageAndOffset = getMockMessageAndOffset(testKafkaRecordByteBuffer);
    Assert.assertEquals(kafkaDecoderExtractor.decodeRecord(mockMessageAndOffset), testKafkaRecord);
    kafkaDecoderExtractor.kafkaConsumerClient.close();
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
  public void testCustomDeserializer() throws Exception {
    
    WorkUnitState mockWorkUnitState = new MockWorkUnitState()
        .withDeserializerName(CustomDeserializer.class.getName())
        .withSchemaRegistryName(SimpleKafkaSchemaRegistry.class.getName());

    KafkaDeserializerExtractor kafkaDecoderExtractor = new KafkaDeserializerExtractor(mockWorkUnitState);
    
    Assert.assertEquals(kafkaDecoderExtractor.getKafkaDeserializer().getDeserializerClass(), CustomDeserializer.class);
    Assert.assertEquals(kafkaDecoderExtractor.getKafkaSchemaRegistry().getClass(), SimpleKafkaSchemaRegistry.class);
    kafkaDecoderExtractor.kafkaConsumerClient.close();
  }
  
  private ByteArrayBasedKafkaRecord getMockMessageAndOffset(ByteBuffer payload) {
    MessageAndOffset mockMessageAndOffset = mock(MessageAndOffset.class);
    Message mockMessage = mock(Message.class);
    when(mockMessage.payload()).thenReturn(payload);
    when(mockMessageAndOffset.message()).thenReturn(mockMessage);
    return new KafkaConsumerClient.Kafka08ConsumerRecord(mockMessageAndOffset);
  }

}