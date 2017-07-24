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

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import gobblin.metrics.kafka.KafkaSchemaRegistry;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;

import lombok.AllArgsConstructor;
import lombok.Getter;


/**
 * Pre-defined Kafka08 {@link Deserializer} that can be referenced by the enum name.
 */
@AllArgsConstructor
@Getter
public enum KafkaDeserializers implements DeserializerType<KafkaDeserializers> {

  CONFLUENT_AVRO(KafkaAvroDeserializer.class, ConfluentKafkaSchemaRegistry.class),

  /**
   * Confluent's JSON {@link Deserializer}
   *
   * @see KafkaJsonDeserializer
   */
  CONFLUENT_JSON(KafkaJsonDeserializer.class, SimpleKafkaSchemaRegistry.class),

  /**
   * A custom {@link Deserializer} for converting <code>byte[]</code> to {@link com.google.gson.JsonElement}s
   *
   * @see KafkaGsonDeserializer
   */
  GSON(KafkaGsonDeserializer.class, SimpleKafkaSchemaRegistry.class),

  /**
   * A standard Kafka {@link Deserializer} that does nothing, it simply returns the <code>byte[]</code>
   */
  BYTE_ARRAY(ByteArrayDeserializer.class, SimpleKafkaSchemaRegistry.class),

  /**
   * A standard Kafka {@link Deserializer} for converting <code>byte[]</code> to {@link String}s
   */
  STRING(StringDeserializer.class, SimpleKafkaSchemaRegistry.class);

  private final Class<? extends Deserializer> deserializerClass;
  private final Class<? extends KafkaSchemaRegistry> schemaRegistryClass;
  
}
