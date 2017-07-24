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

import org.apache.avro.SchemaBuilder;
import org.testng.Assert;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.extract.kafka.KafkaDeserializerTestUtils.MockWorkUnitState;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

public abstract class KafkaDeserializerExtractorTestBase {

  protected static final String TEST_TOPIC_NAME = "testTopic";
  protected static final String TEST_URL = "testUrl";
  protected static final String TEST_RECORD_NAME = "testRecord";
  protected static final String TEST_NAMESPACE = "testNamespace";
  protected static final String TEST_FIELD_NAME = "testField";
  protected static final String TEST_FIELD_NAME2 = "testField2";
      
  public void checkBuiltInStringDeserializerSetup() throws Exception {
    WorkUnitState mockWorkUnitState = new MockWorkUnitState().withDeserializerName("STRING");

    KafkaDeserializerExtractor kafkaDecoderExtractor = new KafkaDeserializerExtractor(mockWorkUnitState);

    Assert.assertEquals(kafkaDecoderExtractor.getKafkaDeserializer().getDeserializerClass(),
        Class.forName("org.apache.kafka.common.serialization.StringDeserializer"));
    Assert.assertEquals(kafkaDecoderExtractor.getKafkaSchemaRegistry().getClass(),
        SimpleKafkaSchemaRegistry.class);
    kafkaDecoderExtractor.kafkaConsumerClient.close();
  }

  public void checkBuiltInGsonDeserializerSetup() throws Exception {
    WorkUnitState mockWorkUnitState = new MockWorkUnitState().withDeserializerName("GSON");

    KafkaDeserializerExtractor kafkaDecoderExtractor = new KafkaDeserializerExtractor(mockWorkUnitState);

    Assert.assertEquals(kafkaDecoderExtractor.getKafkaDeserializer().getDeserializerClass(),
        Class.forName("gobblin.source.extractor.extract.kafka.KafkaGsonDeserializer"));
    Assert.assertEquals(kafkaDecoderExtractor.getKafkaSchemaRegistry().getClass(), SimpleKafkaSchemaRegistry.class);
    kafkaDecoderExtractor.kafkaConsumerClient.close();
  }

  public void checkBuiltInConfluentAvroDeserializerSetup() throws Exception {
    WorkUnitState mockWorkUnitState = new MockWorkUnitState().withDeserializerName("CONFLUENT_AVRO");
    
    KafkaDeserializerExtractor kafkaDecoderExtractor = new KafkaDeserializerExtractor(mockWorkUnitState) {
      @Override
      public Object getSchema() {
        return SchemaBuilder.record(TEST_RECORD_NAME)
            .namespace(TEST_NAMESPACE).fields()
            .name(TEST_FIELD_NAME).type().stringType().noDefault()
            .endRecord();
      }
    };

    Assert.assertEquals(kafkaDecoderExtractor.getKafkaDeserializer().getDeserializerClass(),
        Class.forName("io.confluent.kafka.serializers.KafkaAvroDeserializer"));
    Assert.assertEquals(kafkaDecoderExtractor.getKafkaSchemaRegistry().getClass(),
        ConfluentKafkaSchemaRegistry.class);
    kafkaDecoderExtractor.kafkaConsumerClient.close();
  }

  @AllArgsConstructor
  @NoArgsConstructor
  @EqualsAndHashCode
  @Getter
  @Setter
  protected static class KafkaRecord {

    private String value;
  }
}