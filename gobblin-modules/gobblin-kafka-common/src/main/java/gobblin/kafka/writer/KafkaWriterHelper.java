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

import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

import org.apache.commons.lang3.reflect.ConstructorUtils;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.util.ConfigUtils;
import gobblin.util.PropertiesUtils;
import static gobblin.kafka.writer.KafkaWriterConfigurationKeys.*;
import static gobblin.kafka.writer.KafkaWriterConfigurationKeys.CLIENT_ID_DEFAULT;

import lombok.extern.slf4j.Slf4j;

/**
 * Helper class for version-specific Kafka writers
 */
@Slf4j
public class KafkaWriterHelper {

  static Properties getProducerProperties(Properties props)
  {
    Properties producerProperties = PropertiesUtils.stripPrefix(props, Optional.of(KAFKA_PRODUCER_CONFIG_PREFIX));

    // Provide default properties if not set from above
    producerProperties.putIfAbsent(KEY_SERIALIZER_CONFIG, DEFAULT_KEY_SERIALIZER);
    producerProperties.putIfAbsent(VALUE_SERIALIZER_CONFIG, DEFAULT_VALUE_SERIALIZER);
    producerProperties.putIfAbsent(CLIENT_ID_CONFIG, CLIENT_ID_DEFAULT);
    return producerProperties;
  }

  public static Object getKafkaProducer(Properties props)
  {
    Config config = ConfigFactory.parseProperties(props);
    String kafkaProducerClass = ConfigUtils
        .getString(config, KafkaWriterConfigurationKeys.KAFKA_WRITER_PRODUCER_CLASS,
            KafkaWriterConfigurationKeys.KAFKA_WRITER_PRODUCER_CLASS_DEFAULT);
    Properties producerProps = getProducerProperties(props);
    try {
      Class<?> producerClass = (Class<?>) Class.forName(kafkaProducerClass);
      Object producer = ConstructorUtils.invokeConstructor(producerClass, producerProps);
      return producer;
    } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
      log.error("Failed to instantiate Kafka producer from class " + kafkaProducerClass, e);
      throw Throwables.propagate(e);
    }
  }


}
