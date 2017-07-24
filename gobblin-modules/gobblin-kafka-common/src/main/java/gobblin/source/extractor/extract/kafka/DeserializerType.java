package gobblin.source.extractor.extract.kafka;

import gobblin.metrics.kafka.KafkaSchemaRegistry;


/**
 * Interface for generic access to the Kafka version specific Deserializers
 *
 * @param <E>
 */
public interface DeserializerType<E extends Enum<E>> {

  public Class<?> getDeserializerClass();

  public Class<? extends KafkaSchemaRegistry> getSchemaRegistryClass();
  
}
