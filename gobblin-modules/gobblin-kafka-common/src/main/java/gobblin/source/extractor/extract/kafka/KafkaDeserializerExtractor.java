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
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.serializer.Deserializer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;

import gobblin.annotation.Alias;
import gobblin.configuration.WorkUnitState;
import gobblin.kafka.client.AbstractBaseKafkaConsumerClient;
import gobblin.kafka.client.ByteArrayBasedKafkaRecord;
import gobblin.metrics.kafka.KafkaSchemaRegistry;
import gobblin.metrics.kafka.SchemaRegistryException;
import gobblin.util.AvroUtils;
import gobblin.util.ClassAliasResolver;
import gobblin.util.PropertiesUtils;
import gobblin.util.reflection.GobblinConstructorUtils;
import lombok.AccessLevel;
import lombok.Getter;


/**
 * <p>
 *   Extension of {@link KafkaExtractor} that wraps Kafka's {@link Deserializer} API. Kafka's {@link Deserializer} provides
 *   a generic way of converting Kafka {@link kafka.message.Message}s to {@link Object}. Typically, a {@link Deserializer}
 *   will be used along with a {@link org.apache.kafka.common.serialization.Serializer} which is responsible for converting
 *   an {@link Object} to a Kafka {@link kafka.message.Message}. These APIs are useful for reading and writing to Kafka,
 *   since Kafka is primarily a byte oriented system.
 * </p>
 *
 * <p>
 *   This class wraps the {@link Deserializer} API allowing any existing classes that implement the {@link Deserializer}
 *   API to integrate with seamlessly with Gobblin. The deserializer can be specified in the following ways:
 *
 *   <ul>
 *     <li>{@link #KAFKA_DESERIALIZER_TYPE} can be used to specify a pre-defined enum from {@link Deserializers} or
 *     it can be used to specify the fully-qualified name of a {@link Class} that defines the {@link Deserializer}
 *     interface. If this property is set to a class name, then {@link KafkaSchemaRegistry} must also be specified
 *     using the {@link KafkaSchemaRegistry#KAFKA_SCHEMA_REGISTRY_CLASS} config key</li>
 *   </ul>
 * </p>
 */
@Getter(AccessLevel.PACKAGE)
@Alias(value = "DESERIALIZER")
public class KafkaDeserializerExtractor extends KafkaExtractor<Object, Object> {

  public static final String KAFKA_DESERIALIZER_TYPE = "kafka.deserializer.type";

  private final DeserializerWrapper kafkaDeserializer;
  private final KafkaSchemaRegistry<?, ?> kafkaSchemaRegistry;
  private final Schema latestSchema;

  private final ClassAliasResolver<DeserializerWrapper> aliasResolver =
      new ClassAliasResolver<>(DeserializerWrapper.class);
  
  private static final String DESERIALIZER_WRAPPER_ALIAS = "KafkaDeserializerWrapper";
  
  public KafkaDeserializerExtractor(WorkUnitState state) throws ReflectiveOperationException {
    this(state, Optional.absent(), Optional.absent());
  }

  @VisibleForTesting
  KafkaDeserializerExtractor(WorkUnitState state,
      Optional<DeserializerWrapper> kafkaDeserializer, Optional<KafkaSchemaRegistry<?, ?>> kafkaSchemaRegistry)
          throws ReflectiveOperationException {
    super(state);
    Properties props = getProps(state);
    DeserializerConfig deserializerConfig = 
        ((AbstractBaseKafkaConsumerClient) this.kafkaConsumerClient).getDeserializerConfig();

    this.kafkaDeserializer =
        kafkaDeserializer.isPresent() ? kafkaDeserializer.get() : getKafkaDeserializer(deserializerConfig, props);
    this.kafkaSchemaRegistry =
        kafkaSchemaRegistry.isPresent() ? kafkaSchemaRegistry.get() : getKafkaSchemaRegistry(deserializerConfig, props);
    this.latestSchema = getLatestConfluentAvroSchema(deserializerConfig);
  }
  
  private Schema getLatestConfluentAvroSchema(DeserializerConfig deserializerConfig) {
    Optional<Boolean> isPresent =
        deserializerConfig.getBuiltInDeserializerName().transform(x -> "CONFLUENT_AVRO".equals(x));
    return (isPresent.or(false)) ? (Schema) getSchema() : null;
  }
  
  @Override
  protected Object decodeRecord(ByteArrayBasedKafkaRecord messageAndOffset) throws IOException {
    Object deserialized = kafkaDeserializer.deserialize(this.topicName, messageAndOffset.getMessageBytes());
    return convertToLatestSchema(deserialized);
  }

  @Override
  protected Object convertRecord(Object record) throws IOException {
    return convertToLatestSchema(record);
  }
  
  /**
   * Converts record schema to the latest version if schema originates from Confluent's Schema Registry
   * 
   * @param record GenericRecord whose schema should be converted 
   * @return
   * @throws IOException
   */
  private Object convertToLatestSchema(Object record) throws IOException {
    // Support of schema evolution
    return (this.latestSchema == null) ? record
        : AvroUtils.convertRecordSchema((GenericRecord) record, this.latestSchema);
  }
  
  @Override
  public Object getSchema() {
    try {
      return this.kafkaSchemaRegistry.getLatestSchemaByTopic(this.topicName);
    } catch (SchemaRegistryException e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Instantiates the deserializer wrapper class using the provided {@link DeserializerConfig} object 
   * 
   * @param deserializerConfig DeserializerWrapper object
   * @param props WorkUnit properties
   * @return Wrapper object with the concrete Kafka Deserializer instance
   * 
   * @throws ReflectiveOperationException
   */
  private DeserializerWrapper getKafkaDeserializer(DeserializerConfig deserializerConfig, Properties props)
      throws ReflectiveOperationException {

    DeserializerWrapper deserializer = GobblinConstructorUtils.invokeConstructor(
        DeserializerWrapper.class,
        aliasResolver.resolve(DESERIALIZER_WRAPPER_ALIAS), 
        Class.forName(deserializerConfig.getDeserializerClass()));
    deserializer.configure(PropertiesUtils.propsToStringKeyMap(props), false);
    return deserializer;
  }
  
  /**
   * Instantiates {@link KafkaSchemaRegistry} using the provided {@link DeserializerConfig} object 
   * If not set, it defaults to {@link SimpleKafkaSchemaRegistry}
   * 
   * @param deserializerConfig DeserializerWrapper object
   * @param props WorkUnit properties
   * @return KafkaSchemaRegistry instance
   * 
   * @throws ReflectiveOperationException
   */
  private KafkaSchemaRegistry<?, ?> getKafkaSchemaRegistry(DeserializerConfig deserializerConfig, Properties props)
      throws ReflectiveOperationException {
    Optional<KafkaSchemaRegistry<Object, Object>> schemaReg =
        deserializerConfig.getSchemaRegistryClass().transform(x -> KafkaSchemaRegistry.get(props, x));
    return (schemaReg.isPresent()) ? schemaReg.get() : new SimpleKafkaSchemaRegistry(props);
  }

  /**
   * Gets {@link Properties} from a {@link WorkUnitState} and sets the config <code>schema.registry.url</code> to value
   * of {@link KafkaSchemaRegistry#KAFKA_SCHEMA_REGISTRY_URL} if set. This way users don't need to specify both
   * properties as <code>schema.registry.url</code> is required by the {@link ConfluentKafkaSchemaRegistry}.
   */
  private static Properties getProps(WorkUnitState workUnitState) {
    Properties properties = workUnitState.getProperties();
    if (properties.containsKey(KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_URL)) {
      properties.setProperty(ConfluentKafkaSchemaRegistry.CONFLUENT_SCHEMA_REGISTRY_URL,
          properties.getProperty(KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_URL));
    }
    return properties;
  }
}
