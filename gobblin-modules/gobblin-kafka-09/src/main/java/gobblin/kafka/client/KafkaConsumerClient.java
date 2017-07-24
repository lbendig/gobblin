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
package gobblin.kafka.client;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import gobblin.annotation.Alias;
import gobblin.configuration.ConfigurationKeys;
import gobblin.kafka.client.KafkaConsumerClient.Factory;
import gobblin.metrics.kafka.KafkaSchemaRegistry;
import gobblin.source.extractor.extract.kafka.ConfluentKafkaSchemaRegistry;
import gobblin.source.extractor.extract.kafka.DeserializerType;
import gobblin.source.extractor.extract.kafka.KafkaDeserializerExtractor;
import gobblin.source.extractor.extract.kafka.KafkaDeserializers;
import gobblin.source.extractor.extract.kafka.KafkaOffsetRetrievalFailureException;
import gobblin.source.extractor.extract.kafka.KafkaPartition;
import gobblin.source.extractor.extract.kafka.KafkaTopic;
import gobblin.util.ConfigUtils;
import gobblin.util.DatasetFilterUtils;

import lombok.EqualsAndHashCode;
import lombok.ToString;


/**
 * A {@link GobblinKafkaConsumerClient} that uses kafka 09 consumer client. Use {@link Factory#create(Config)} to create
 * new KafkaConsumerClients. The {@link Config} used to create clients must have required key {@link AbstractBaseKafkaConsumerClient#GOBBLIN_CONFIG_VALUE_DESERIALIZER_CLASS_KEY}
 *
 * @param <K> Message key type
 * @param <V> Message value type
 */
public class KafkaConsumerClient<K, V> extends AbstractBaseKafkaConsumerClient {

  private static final String KAFKA_DEFAULT_ENABLE_AUTO_COMMIT = Boolean.toString(false);
  private static final String KAFKA_DEFAULT_KEY_DESERIALIZER = StringDeserializer.class.getName();

  private final Consumer<K, V> consumer;

  @VisibleForTesting
  protected KafkaConsumerClient(Config config, Optional<Consumer<K, V>> consumer) {
    super(config);
    this.consumer = consumer.isPresent() ? consumer.get() : new KafkaConsumer<>(getConsumerConfig(config));
  }
  
  private KafkaConsumerClient(Config config) {
    this(config, Optional.absent());
  }

  private Properties getConsumerConfig(Config config) {
    Properties props = new Properties();
    props.put(KAFKA_CLIENT_KEY_DESERIALIZER_CLASS_KEY,
        ConfigUtils.getString(config, GOBBLIN_CONFIG_KEY_DESERIALIZER_CLASS_KEY, KAFKA_DEFAULT_KEY_DESERIALIZER));
    props.put(KAFKA_CLIENT_VALUE_DESERIALIZER_CLASS_KEY, getDeserializerConfig().getDeserializerClass());
    if (config.hasPath(KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_URL)) {
      props.put(ConfluentKafkaSchemaRegistry.CONFLUENT_SCHEMA_REGISTRY_URL,
          config.getString(KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_URL));
    }
    props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getString(ConfigurationKeys.JOB_ID_KEY));
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Joiner.on(",").join(super.brokers));
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, KAFKA_DEFAULT_ENABLE_AUTO_COMMIT);
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, super.socketTimeoutMillis);
    return props;
  }

  public KafkaConsumerClient(Config config, Consumer<K, V> consumer) {
    super(config);
    this.consumer = consumer;
  }

  @Override
  public List<KafkaTopic> getTopics() {
    return FluentIterable.from(this.consumer.listTopics().entrySet())
        .transform(new Function<Entry<String, List<PartitionInfo>>, KafkaTopic>() {
          @Override
          public KafkaTopic apply(Entry<String, List<PartitionInfo>> filteredTopicEntry) {
            return new KafkaTopic(filteredTopicEntry.getKey(), Lists.transform(filteredTopicEntry.getValue(),
                PARTITION_INFO_TO_KAFKA_PARTITION));
          }
        }).toList();
  }

  @Override
  public long getEarliestOffset(KafkaPartition partition) throws KafkaOffsetRetrievalFailureException {
    throw new UnsupportedOperationException("getEarliestOffset and getLatestOffset is not supported by Kafka-09");
  }

  @Override
  public long getLatestOffset(KafkaPartition partition) throws KafkaOffsetRetrievalFailureException {
    throw new UnsupportedOperationException("getEarliestOffset and getLatestOffset is not supported by Kafka-09");
  }

  @Override
  public Iterator<KafkaConsumerRecord> consume(KafkaPartition partition, long nextOffset, long maxOffset) {

    if (nextOffset > maxOffset) {
      return null;
    }

    this.consumer.assign(Lists.newArrayList(new TopicPartition(partition.getTopicName(), partition.getId())));
    this.consumer.seek(new TopicPartition(partition.getTopicName(), partition.getId()), nextOffset);
    ConsumerRecords<K, V> consumerRecords = consumer.poll(super.fetchTimeoutMillis);
    return Iterators.transform(consumerRecords.iterator(), new Function<ConsumerRecord<K, V>, KafkaConsumerRecord>() {

      @Override
      public KafkaConsumerRecord apply(ConsumerRecord<K, V> input) {
        return new Kafka09ConsumerRecord<>(input);
      }
    });
  }

  @Override
  public void close() throws IOException {
    this.consumer.close();
  }

  private static final Function<PartitionInfo, KafkaPartition> PARTITION_INFO_TO_KAFKA_PARTITION =
      new Function<PartitionInfo, KafkaPartition>() {
        @Override
        public KafkaPartition apply(@Nonnull PartitionInfo partitionInfo) {
          return new KafkaPartition.Builder().withId(partitionInfo.partition()).withTopicName(partitionInfo.topic())
              .withLeaderId(partitionInfo.leader().id())
              .withLeaderHostAndPort(partitionInfo.leader().host(), partitionInfo.leader().port()).build();
        }
      };

  /**
    * A factory class to instantiate {@link KafkaConsumerClient}
   */
  public static class Factory implements GobblinKafkaConsumerClientFactory {
    @SuppressWarnings("rawtypes")
    @Override
    public GobblinKafkaConsumerClient create(Config config) {
      return new KafkaConsumerClient(config);
    }
  }

  /**
   * A record returned by {@link KafkaConsumerClient}
   *
   * @param <K> Message key type
   * @param <V> Message value type
   */
  @EqualsAndHashCode(callSuper = true)
  @ToString
  public static class Kafka09ConsumerRecord<K, V> extends BaseKafkaConsumerRecord implements
      DecodeableKafkaRecord<K, V> {
    private final ConsumerRecord<K, V> consumerRecord;

    public Kafka09ConsumerRecord(ConsumerRecord<K, V> consumerRecord) {
      // Kafka 09 consumerRecords do not provide value size.
      // Only 08 and 10 versions provide them.
      super(consumerRecord.offset(), BaseKafkaConsumerRecord.VALUE_SIZE_UNAVAILABLE);
      this.consumerRecord = consumerRecord;
    }

    @Override
    public K getKey() {
      return this.consumerRecord.key();
    }

    @Override
    public V getValue() {
      return this.consumerRecord.value();
    }
  }
  
  @Override
  public Optional<DeserializerType<?>> getDeserializerType(String deserializerName) {
    try {
      return Optional.of(Enum.valueOf(KafkaDeserializers.class, deserializerName.toUpperCase()));
    } catch (IllegalArgumentException iae) {
      return Optional.absent();
    }
  }
  
}
