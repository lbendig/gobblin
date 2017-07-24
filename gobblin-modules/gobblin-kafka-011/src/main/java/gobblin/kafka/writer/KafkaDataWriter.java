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

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.writer.AsyncDataWriter;
import gobblin.writer.WriteCallback;
import gobblin.writer.WriteResponse;
import gobblin.writer.WriteResponseFuture;
import gobblin.writer.WriteResponseMapper;

import lombok.extern.slf4j.Slf4j;


/**
 * Implementation of KafkaWriter that wraps a {@link KafkaProducer}.
 * This provides at-least once semantics.
 * Applications should expect data to be possibly written to Kafka even if the overall Gobblin job fails.
 *
 */
@Slf4j
public class KafkaDataWriter<D> implements AsyncDataWriter<D> {

  private static final WriteResponseMapper<RecordMetadata> WRITE_RESPONSE_WRAPPER =
      new WriteResponseMapper<RecordMetadata>() {

        @Override
        public WriteResponse<RecordMetadata> wrap(final RecordMetadata recordMetadata) {
          return new WriteResponse<RecordMetadata>() {
            @Override
            public RecordMetadata getRawResponse() {
              return recordMetadata;
            }

            @Override
            public String getStringResponse() {
              return recordMetadata.toString();
            }

            @Override
            public long bytesWritten() {
              return recordMetadata.serializedKeySize() + recordMetadata.serializedValueSize();
            }
          };
        }
      };

  private final Producer<String, D> producer;
  private final String topic;

  public static <D> Producer<String, D> getKafkaProducer(Properties props) {
    return (Producer<String, D>)KafkaWriterHelper.getKafkaProducer(props);
  }

  public KafkaDataWriter(Properties props) {
    this(getKafkaProducer(props), ConfigFactory.parseProperties(props));
  }

  public KafkaDataWriter(Producer<String, D> producer, Config config) {
    this.topic = config.getString(KafkaWriterConfigurationKeys.KAFKA_TOPIC);
    this.producer = producer;
  }

  @Override
  public void close() throws IOException {
    log.debug("Close called");
    this.producer.close();
  }

  @Override
  public Future<WriteResponse> write(final D record, final WriteCallback callback) {
    return new WriteResponseFuture<>(this.producer.send(new ProducerRecord<String, D>(topic, record), 
        (final RecordMetadata metadata, Exception exception) -> {
          if (exception != null) {
            callback.onFailure(exception);
          } else {
            callback.onSuccess(WRITE_RESPONSE_WRAPPER.wrap(metadata));
          }
        }), WRITE_RESPONSE_WRAPPER);
  }
  
  @Override
  public void flush() throws IOException {
    this.producer.flush();
  }
}
