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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import gobblin.kafka.client.AbstractBaseKafkaConsumerClient;
import gobblin.metrics.kafka.KafkaSchemaRegistry;

import lombok.Getter;

/**
 * Configures the Deserializer and associated Schema Registry (if any) classes for a job
 *
 */
@Getter
public class DeserializerConfig {

  private final Optional<String> builtInDeserializerName;
  private final String deserializerClass;
  private final Optional<String> schemaRegistryClass;
  
  /**
   * @param config 
   * @param deserializerType set through KafkaDeserializerExtractor.KAFKA_DESERIALIZER_TYPE
   */
  public DeserializerConfig(Config config, Optional<DeserializerType<?>> deserializerType) {
    this.builtInDeserializerName = deserializerType.transform(DeserializerType::toString);
    this.deserializerClass = getDeserializerClass(config, deserializerType);
    this.schemaRegistryClass = getSchemaRegistryClass(config, deserializerType);
  }
  
  /**
   * Returns the Deserializer class either from {@link KafkaDeserializerExtractor.KAFKA_DESERIALIZER_TYPE} or
   * from {@link GOBBLIN_CONFIG_VALUE_DESERIALIZER_CLASS_KEY}. The last one takes precedence.
   * 
   * @param config
   * @param deserializerType
   * @return
   */
  private String getDeserializerClass(Config config, Optional<DeserializerType<?>> deserializerType) {
    // Custom deserializer
    if (config.hasPath(AbstractBaseKafkaConsumerClient.GOBBLIN_CONFIG_VALUE_DESERIALIZER_CLASS_KEY)) {
      return config.getString(AbstractBaseKafkaConsumerClient.GOBBLIN_CONFIG_VALUE_DESERIALIZER_CLASS_KEY);
    }

    // Predefined deserializer taken from the concrete KafkaDeserializers enum
    Optional<String> preset = deserializerType.transform(x -> x.getDeserializerClass().getName());
    if (preset.isPresent()) {
      return preset.get();
    }
    
    // If deserializer type couldn't be resolved, its value is treated as a fully qualified derserializer class name
    if (config.hasPath(KafkaDeserializerExtractor.KAFKA_DESERIALIZER_TYPE)) {
      Preconditions.checkArgument(getSchemaRegistryClass(config, deserializerType).isPresent(),
          "Unable to find %s for custom %s", KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_CLASS,
          KafkaDeserializerExtractor.KAFKA_DESERIALIZER_TYPE);
      return config.getString(KafkaDeserializerExtractor.KAFKA_DESERIALIZER_TYPE);
    }
    throw new RuntimeException("Missing " + KafkaDeserializerExtractor.KAFKA_DESERIALIZER_TYPE);
  }

  private Optional<String> getSchemaRegistryClass(Config config, Optional<DeserializerType<?>> deserializerType) {
    if (config.hasPath(KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_CLASS)) {
      return Optional.of(config.getString(KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_CLASS));
    }
    return deserializerType.transform(x -> x.getSchemaRegistryClass().getName());
  }
  
}
