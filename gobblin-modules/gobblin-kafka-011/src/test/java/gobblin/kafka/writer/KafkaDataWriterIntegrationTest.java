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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.util.Iterator;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.io.Closer;

import gobblin.kafka.KafkaTestBase;
import gobblin.kafka.client.KafkaConsumerClient.Kafka011ConsumerRecord;
import gobblin.runtime.JobLauncher;
import gobblin.runtime.JobLauncherFactory;
import gobblin.source.extractor.extract.kafka.KafkaDeserializers;
import static gobblin.kafka.KafkaTestBase.prodPrefix;

import lombok.extern.slf4j.Slf4j;


/**
 * Tests that set up a complete standalone Gobblin pipeline along with a Kafka suite
 */
@Slf4j
public class KafkaDataWriterIntegrationTest {

  private static final String JOB_PROPS_DIR = "gobblin-modules/gobblin-kafka-011/resource/job-props/";
  private static final String TEST_LAUNCHER_PROPERTIES_FILE = JOB_PROPS_DIR + "testKafkaIngest.properties";
  private static final String TEST_INGEST_PULL_FILE = JOB_PROPS_DIR + "testKafkaIngest.pull";
  
  private KafkaTestBase kafkaTestHelper;
  private Properties gobblinProps;
  private Properties jobProps;
  private int numRecordsPerExtract = 5;
  private int numParallel = 2;
  
  private static final String TOPIC = KafkaDataWriterIntegrationTest.class.getName();

  @BeforeClass
  public void setup() throws Exception {

    this.kafkaTestHelper = new KafkaTestBase();
    this.gobblinProps = new Properties();
    this.gobblinProps.load(new FileReader(TEST_LAUNCHER_PROPERTIES_FILE));

    this.jobProps = new Properties();
    this.jobProps.load(new FileReader(TEST_INGEST_PULL_FILE));

    replaceProperties(this.jobProps, "{$topic}", TOPIC);
    replaceProperties(this.jobProps, "{$kafkaPort}", "" + this.kafkaTestHelper.getKafkaServerPort());
    this.jobProps.setProperty("source.numRecordsPerExtract", "" + this.numRecordsPerExtract);
    this.jobProps.setProperty("source.numParallelism", "" + this.numParallel);
    this.jobProps.setProperty("job.commit.policy", "partial");
    this.jobProps.setProperty("publish.at.job.level", "false");
    
    this.kafkaTestHelper.startServers();
  }

  @AfterClass
  public void stopServers() throws IOException {
    this.kafkaTestHelper.close();
  }

  @AfterClass
  @BeforeClass
  public void cleanup() throws Exception {
    File file = new File("gobblin-kafka/testOutput");
    FileUtils.deleteDirectory(file);
  }
  
  @Test
  public void testErrors() throws Exception {
    log.warn("Process id = " + ManagementFactory.getRuntimeMXBean().getName());
    int errorEvery = 2000;
    int totalRecords = this.numRecordsPerExtract * this.numParallel;
    int totalSuccessful = totalRecords / errorEvery + totalRecords % errorEvery;
    {
      Closer closer = Closer.create();
      try {

        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:" + this.kafkaTestHelper.getKafkaServerPort());
        this.kafkaTestHelper.provisionTopic(TOPIC);

        // all records from partition 0 will be dropped.
        this.jobProps.setProperty(prodPrefix.apply("flaky.errorType"), "regex");
        this.jobProps.setProperty(prodPrefix.apply("flaky.regexPattern"), ":index:0.*");

        totalSuccessful = 5; // number of records in partition 1
        JobLauncher jobLauncher = closer.register(JobLauncherFactory.newJobLauncher(this.gobblinProps, this.jobProps));
        jobLauncher.launchJob(null);
      } catch (Exception e) {
        log.error("Failed to run job with exception ", e);
        Assert.fail("Should not throw exception on running the job", e);
      } finally {
        closer.close();
      }
      // test records written
      testRecordsWritten(totalSuccessful, TOPIC);
    }
    boolean trySecond = true;
    if (trySecond) {
      Closer closer = Closer.create();
      try {
        
        this.jobProps.setProperty(prodPrefix.apply("flaky.errorType"), "nth");
        this.jobProps.setProperty(prodPrefix.apply("flaky.errorEvery"), "" + errorEvery);
        
        JobLauncher jobLauncher = closer.register(JobLauncherFactory.newJobLauncher(this.gobblinProps, this.jobProps));
        jobLauncher.launchJob(null);
        totalSuccessful = totalRecords / errorEvery + totalRecords % errorEvery;
      } catch (Exception e) {
        log.error("Failed to run job with exception ", e);
        Assert.fail("Should not throw exception on running the job", e);
      } finally {
        closer.close();
      }
    }
    // test records written
    testRecordsWritten(totalSuccessful, TOPIC);
  }

  private void replaceProperties(Properties props, String searchString, String replacementString) {
    props.forEach((key, value) -> props.replace(key, ((String) value).replace(searchString, replacementString)));
  }

  private void testRecordsWritten(int totalSuccessful, String topicName) throws UnsupportedEncodingException {
    Properties consumerProps = this.kafkaTestHelper.getConsumerProps(KafkaDeserializers.STRING);
    Iterator<Kafka011ConsumerRecord<String, String>> it = this.kafkaTestHelper.getIterator(topicName, consumerProps);
    for (int i = 0; i < totalSuccessful; i++) {
      log.debug(String.format("%d of %d: Message consumed: %s", (i++), totalSuccessful, it.next().getValue()));
    }
  }
  
}
