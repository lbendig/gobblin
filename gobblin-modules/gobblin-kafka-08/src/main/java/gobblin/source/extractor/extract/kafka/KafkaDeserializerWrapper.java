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

import java.util.Map;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.kafka.common.serialization.Deserializer;

import com.google.common.annotations.VisibleForTesting;

import gobblin.annotation.Alias;

/**
 * Wraps Kafka08 Deserializer for a generic access in KafkaDeserializerExtractor
 *
 */
@Alias(value = "KafkaDeserializerWrapper")
public class KafkaDeserializerWrapper implements DeserializerWrapper {

  private Deserializer<?> deserializer;

  public KafkaDeserializerWrapper(Class<?> deserializerClass)
      throws ReflectiveOperationException {
    this.deserializer = (Deserializer<?>) ConstructorUtils.invokeConstructor(deserializerClass);
  }

  @VisibleForTesting
  KafkaDeserializerWrapper(Deserializer<?> deserializer) {
    this.deserializer = deserializer;
  }
  
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    deserializer.configure(configs, isKey);
  }

  @Override
  public Object deserialize(String topic, byte[] data) {
    return deserializer.deserialize(topic, data);
  }

  @Override
  public Class<?> getDeserializerClass() {
    return deserializer.getClass();
  }
  
  @Override
  public void close() {
    deserializer.close();
  }

}
