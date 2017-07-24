package gobblin.source.extractor.extract.kafka;

import static org.mockito.Mockito.mock;

import java.util.ArrayList;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.metrics.kafka.KafkaSchemaRegistry;
import gobblin.source.extractor.WatermarkInterval;
import gobblin.source.workunit.WorkUnit;

public class KafkaDeserializerTestUtils {

  public static class MockWorkUnitState extends WorkUnitState {

    private static final String TEST_TOPIC_NAME = "testTopic";
    private static final String TEST_URL = "testUrl";
    
    public MockWorkUnitState() {
      this(Optional.absent());
    }
    
    public MockWorkUnitState(Optional<WatermarkInterval> watermarkInterval) {
      super(initWorkUnit(watermarkInterval), new State());
      this.setProp(KafkaSource.TOPIC_NAME, TEST_TOPIC_NAME);
      this.setProp(KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_URL, TEST_URL);
      this.setProp(KafkaSource.PARTITION_ID, 0);
      this.setProp(KafkaSource.LEADER_ID, 0);
      this.setProp(KafkaSource.LEADER_HOSTANDPORT, 0);
      this.setProp(ConfigurationKeys.KAFKA_BROKERS, "localhost:8080");
      this.setProp(ConfigurationKeys.JOB_ID_KEY, "testJobId");
    }

    private static WorkUnit initWorkUnit(Optional<WatermarkInterval> watermarkInterval) {
      WorkUnit mockWorkUnit = WorkUnit.createEmpty();
      mockWorkUnit.setWatermarkInterval(watermarkInterval.or(
          new WatermarkInterval(new MultiLongWatermark(new ArrayList<Long>()), 
              new MultiLongWatermark(new ArrayList<Long>()))));
      return mockWorkUnit;
    }

    public MockWorkUnitState withDeserializerName(String deserializerName) {
      this.setProp(KafkaDeserializerExtractor.KAFKA_DESERIALIZER_TYPE, deserializerName);
      return this;
    }

    public MockWorkUnitState withSchemaRegistryName(String schemaRegistryClass) {
      this.setProp(KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_CLASS, schemaRegistryClass);
      return this;
    }
    
    public MockWorkUnitState withConsumerFactoryClass(Class<?> factoryClass) {
      this.setProp(KafkaSource.GOBBLIN_KAFKA_CONSUMER_CLIENT_FACTORY_CLASS, factoryClass.getName());
      return this;
    }
    
    public MockWorkUnitState with(String key, Object value) {
      this.setProp(key, value);
      return this;
    }

  }
  
  public static KafkaDeserializerExtractor getMockDeserializerExtractor(WorkUnitState mockWorkUnitState,
      DeserializerWrapper deserializer, Optional<KafkaSchemaRegistry<?, ?>> mockKafkaSchemaRegistry) {
    try {
      return new KafkaDeserializerExtractor(mockWorkUnitState, Optional.of(deserializer),
          (mockKafkaSchemaRegistry.isPresent() ? mockKafkaSchemaRegistry
              : Optional.of(mock(KafkaSchemaRegistry.class))));
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
  
}
