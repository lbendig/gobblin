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

import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;

import gobblin.kafka.client.AbstractBaseKafkaConsumerClient;
import gobblin.metrics.kafka.KafkaSchemaRegistry;
import gobblin.util.ConfigUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;


@Test(groups = { "gobblin.source.extractor.extract.kafka" })
public class DeserializerConfigTest {

  private static final String DUMMY_REGISTRY = "testSchemaRegistryClass";
  private static final String DUMMY_DESERIALIZER = "testDeserializerClass";
  
  @AllArgsConstructor
  @Getter
  private static enum DummyKafkaDeserializers implements DeserializerType<DummyKafkaDeserializers> {

    STRING(String.class, ConfluentKafkaSchemaRegistry.class),
    INTEGER(Integer.class, SimpleKafkaSchemaRegistry.class);

    private final Class<?> deserializerClass;
    private final Class<? extends KafkaSchemaRegistry<?,?>> schemaRegistryClass;

  }

  @Test
  public void testDeserializerConfigWithPreset() throws Exception {
    
    Optional<DeserializerType<?>> deserializerType = Optional.of(DummyKafkaDeserializers.STRING);
    DeserializerConfig conf = new DeserializerConfig(ConfigUtils.propertiesToConfig(new Properties()), deserializerType);  
    
    Assert.assertEquals(Optional.of("STRING"), conf.getBuiltInDeserializerName());
    Assert.assertEquals(String.class.getName(), conf.getDeserializerClass());
    Assert.assertEquals(Optional.of(ConfluentKafkaSchemaRegistry.class.getName()), conf.getSchemaRegistryClass());
  }
  
  @Test
  public void testDeserializerConfigWithCustom1() throws Exception {
    Properties props = new Properties();
    props.setProperty(KafkaDeserializerExtractor.KAFKA_DESERIALIZER_TYPE, DUMMY_DESERIALIZER);
    props.setProperty(KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_CLASS, DUMMY_REGISTRY);
    DeserializerConfig conf = new DeserializerConfig(ConfigUtils.propertiesToConfig(props), Optional.absent());  

    Assert.assertFalse(conf.getBuiltInDeserializerName().isPresent());
    Assert.assertEquals(DUMMY_DESERIALIZER, conf.getDeserializerClass());
    Assert.assertEquals(Optional.of(DUMMY_REGISTRY), conf.getSchemaRegistryClass());
  }
  
  @Test
  public void testDeserializerConfigWithCustom2() throws Exception {
    Properties props = new Properties();
    props.setProperty(KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_CLASS, DUMMY_REGISTRY);
    
    Optional<DeserializerType<?>> deserializerType = Optional.of(DummyKafkaDeserializers.INTEGER);
    DeserializerConfig conf = new DeserializerConfig(ConfigUtils.propertiesToConfig(props),deserializerType);  

    Assert.assertEquals(Optional.of("INTEGER"), conf.getBuiltInDeserializerName());
    Assert.assertEquals(Integer.class.getName(), conf.getDeserializerClass());
    Assert.assertEquals(Optional.of(DUMMY_REGISTRY), conf.getSchemaRegistryClass());
  }
  
  @Test
  public void testDeserializerConfigWithCustom3() throws Exception {
    Properties props = new Properties();
    props.setProperty(AbstractBaseKafkaConsumerClient.GOBBLIN_CONFIG_VALUE_DESERIALIZER_CLASS_KEY, DUMMY_DESERIALIZER);
    
    Optional<DeserializerType<?>> deserializerType = Optional.of(DummyKafkaDeserializers.STRING);
    DeserializerConfig conf = new DeserializerConfig(ConfigUtils.propertiesToConfig(props),deserializerType);  

    Assert.assertEquals(Optional.of("STRING"), conf.getBuiltInDeserializerName());
    Assert.assertEquals(DUMMY_DESERIALIZER, conf.getDeserializerClass());
    Assert.assertEquals(Optional.of(ConfluentKafkaSchemaRegistry.class.getName()), conf.getSchemaRegistryClass());
  }
  
  @Test
  public void testDeserializerConfigError1() throws Exception {
    Properties props = new Properties();
    props.setProperty(KafkaDeserializerExtractor.KAFKA_DESERIALIZER_TYPE, DUMMY_DESERIALIZER);
    try {
      new DeserializerConfig(ConfigUtils.propertiesToConfig(props), Optional.absent());
      Assert.fail("DeserializerConfig must not be initialized without schema registry configuration");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(e.getMessage(), String.format("Unable to find %s for custom %s",
          KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_CLASS, KafkaDeserializerExtractor.KAFKA_DESERIALIZER_TYPE));
    }
  }
  
  @Test
  public void testDeserializerConfigError2() throws Exception {
    Properties props = new Properties();
    try {
      new DeserializerConfig(ConfigUtils.propertiesToConfig(props), Optional.absent());
      Assert.fail("DeserializerConfig must not be initialized without deserializer configuration");
    } catch (RuntimeException e) {
      Assert.assertEquals(e.getMessage(), "Missing kafka.deserializer.type");
    }
  }

}
