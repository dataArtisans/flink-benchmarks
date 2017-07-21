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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.NetUtils;

import kafka.admin.AdminUtils;
import kafka.common.KafkaException;
import kafka.metrics.KafkaMetricsReporter;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.commons.collections.list.UnmodifiableList;
import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.BindException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import scala.collection.mutable.ArraySeq;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.flink.util.NetUtils.hostAndPortToUrlString;

/**
 * An implementation of the KafkaServerProvider for Kafka 0.11 .
 */
public class KafkaTestEnvironmentImpl {

	protected static final Logger LOG = LoggerFactory.getLogger(KafkaTestEnvironmentImpl.class);
	protected static final String KAFKA_HOST = "localhost";

	private File tmpZkDir;
	private File tmpKafkaParent;
	private List<File> tmpKafkaDirs;
	private List<KafkaServer> brokers;
	private TestingServer zookeeper;
	private String zookeeperConnectionString;
	private String brokerConnectionString = "";
	private Properties standardProps;
	private Properties additionalServerProperties;
	private boolean secureMode = false;
	// 6 seconds is default. Seems to be too small for travis. 30 seconds
	private int zkTimeout = 30000;
	private FlinkKafkaProducer011.Semantic producerSemantic = FlinkKafkaProducer011.Semantic.EXACTLY_ONCE;

	public String getBrokerConnectionString() {
		return brokerConnectionString;
	}

	public void setProducerSemantic(FlinkKafkaProducer011.Semantic producerSemantic) {
		this.producerSemantic = producerSemantic;
	}

	public Properties getStandardProperties() {
		return standardProps;
	}

	public Properties getSecureProperties() {
		Properties prop = new Properties();
		if (secureMode) {
			prop.put("security.inter.broker.protocol", "SASL_PLAINTEXT");
			prop.put("security.protocol", "SASL_PLAINTEXT");
			prop.put("sasl.kerberos.service.name", "kafka");
		}
		return prop;
	}

	public String getVersion() {
		return "0.11";
	}

	public List<KafkaServer> getBrokers() {
		return brokers;
	}

	public <T> FlinkKafkaConsumerBase<T> getConsumer(List<String> topics, KeyedDeserializationSchema<T> readSchema, Properties props) {
		return new FlinkKafkaConsumer011<>(topics, readSchema, props);
	}

	public <K, V> Collection<ConsumerRecord<K, V>> getAllRecordsFromTopic(Properties properties, String topic, int partition, long timeout) {
		List<ConsumerRecord<K, V>> result = new ArrayList<>();

		try (KafkaConsumer<K, V> consumer = new KafkaConsumer<>(properties)) {
			consumer.assign(Arrays.asList(new TopicPartition(topic, partition)));

			while (true) {
				boolean processedAtLeastOneRecord = false;

				// wait for new records with timeout and break the loop if we didn't get any
				Iterator<ConsumerRecord<K, V>> iterator = consumer.poll(timeout).iterator();
				while (iterator.hasNext()) {
					ConsumerRecord<K, V> record = iterator.next();
					result.add(record);
					processedAtLeastOneRecord = true;
				}

				if (!processedAtLeastOneRecord) {
					break;
				}
			}
			consumer.commitSync();
		}

		return UnmodifiableList.decorate(result);
	}

	public <T> StreamSink<T> getProducerSink(String topic, KeyedSerializationSchema<T> serSchema, Properties props, FlinkKafkaPartitioner<T> partitioner) {
		FlinkKafkaProducer011<T> prod = new FlinkKafkaProducer011<>(topic, serSchema, props, partitioner, producerSemantic);
		return new StreamSink<>(prod);
	}

	public <T> DataStreamSink<T> produceIntoKafka(DataStream<T> stream, String topic, KeyedSerializationSchema<T> serSchema, Properties props, FlinkKafkaPartitioner<T> partitioner) {
		FlinkKafkaProducer011<T> prod = new FlinkKafkaProducer011<>(topic, serSchema, props, partitioner, producerSemantic);
		return stream.addSink(prod);
	}

	public <T> DataStreamSink<T> writeToKafkaWithTimestamps(DataStream<T> stream, String topic, KeyedSerializationSchema<T> serSchema, Properties props) {
		return FlinkKafkaProducer011.writeToKafkaWithTimestamps(stream, topic, serSchema, props, new FlinkFixedPartitioner<T>(), producerSemantic);
	}

	public void restartBroker(int leaderId) throws Exception {
		brokers.set(leaderId, getKafkaServer(leaderId, tmpKafkaDirs.get(leaderId)));
	}

	public int getLeaderToShutDown(String topic) throws Exception {
		ZkUtils zkUtils = getZkUtils();
		try {
			MetadataResponse.PartitionMetadata firstPart = null;
			do {
				if (firstPart != null) {
					LOG.info("Unable to find leader. error code {}", firstPart.error().code());
					// not the first try. Sleep a bit
					Thread.sleep(150);
				}

				List<MetadataResponse.PartitionMetadata> partitionMetadata = AdminUtils.fetchTopicMetadataFromZk(topic, zkUtils).partitionMetadata();
				firstPart = partitionMetadata.get(0);
			}
			while (firstPart.error().code() != 0);

			return firstPart.leader().id();
		} finally {
			zkUtils.close();
		}
	}

	public int getBrokerId(KafkaServer server) {
		return server.config().brokerId();
	}

	public boolean isSecureRunSupported() {
		return true;
	}

	public void prepare(int numKafkaServers, Properties additionalServerProperties, boolean secureMode) {
		//increase the timeout since in Travis ZK connection takes long time for secure connection.
		if (secureMode) {
			//run only one kafka server to avoid multiple ZK connections from many instances - Travis timeout
			numKafkaServers = 1;
			zkTimeout = zkTimeout * 15;
		}

		this.additionalServerProperties = additionalServerProperties;
		this.secureMode = secureMode;
		File tempDir = new File(System.getProperty("java.io.tmpdir"));

		tmpZkDir = new File(tempDir, "kafkaITcase-zk-dir-" + (UUID.randomUUID().toString()));
		checkState(tmpZkDir.mkdirs(), "cannot create zookeeper temp dir");

		tmpKafkaParent = new File(tempDir, "kafkaITcase-kafka-dir*" + (UUID.randomUUID().toString()));
		checkState(tmpKafkaParent.mkdirs(), "cannot create kafka temp dir");

		tmpKafkaDirs = new ArrayList<>(numKafkaServers);
		for (int i = 0; i < numKafkaServers; i++) {
			File tmpDir = new File(tmpKafkaParent, "server-" + i);
			checkState(tmpDir.mkdir(), "cannot create kafka temp dir");
			tmpKafkaDirs.add(tmpDir);
		}

		zookeeper = null;
		brokers = null;

		try {
			zookeeper = new TestingServer(-1, tmpZkDir);
			zookeeperConnectionString = zookeeper.getConnectString();
			LOG.info("Starting Zookeeper with zookeeperConnectionString: {}", zookeeperConnectionString);

			LOG.info("Starting KafkaServer");
			brokers = new ArrayList<>(numKafkaServers);

			for (int i = 0; i < numKafkaServers; i++) {
				brokers.add(getKafkaServer(i, tmpKafkaDirs.get(i)));

				if (secureMode) {
					brokerConnectionString += hostAndPortToUrlString(
						KAFKA_HOST,
						brokers.get(i)
							.socketServer()
							.boundPort(ListenerName.forSecurityProtocol(SecurityProtocol.SASL_PLAINTEXT)));
				} else {
					brokerConnectionString += hostAndPortToUrlString(
						KAFKA_HOST,
						brokers.get(i)
							.socketServer()
							.boundPort(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)));
				}
				brokerConnectionString += ",";
			}

			LOG.info("ZK and KafkaServer started.");
		}
		catch (Throwable t) {
			t.printStackTrace();
			throw new IllegalStateException("Test setup failed: " + t.getMessage(), t);
		}

		standardProps = new Properties();
		standardProps.setProperty("zookeeper.connect", zookeeperConnectionString);
		standardProps.setProperty("bootstrap.servers", brokerConnectionString);
		standardProps.setProperty("group.id", "flink-tests");
		standardProps.setProperty("enable.auto.commit", "false");
		standardProps.setProperty("zookeeper.session.timeout.ms", String.valueOf(zkTimeout));
		standardProps.setProperty("zookeeper.connection.timeout.ms", String.valueOf(zkTimeout));
		standardProps.setProperty("auto.offset.reset", "earliest"); // read from the beginning. (earliest is kafka 0.11 value)
		standardProps.setProperty("max.partition.fetch.bytes", "256"); // make a lot of fetches (MESSAGES MUST BE SMALLER!)
	}

	public void shutdown() {
		for (KafkaServer broker : brokers) {
			if (broker != null) {
				broker.shutdown();
			}
		}
		brokers.clear();

		if (zookeeper != null) {
			try {
				zookeeper.stop();
			}
			catch (Exception e) {
				LOG.warn("ZK.stop() failed", e);
			}
			zookeeper = null;
		}

		// clean up the temp spaces

		if (tmpKafkaParent != null && tmpKafkaParent.exists()) {
			try {
				FileUtils.deleteDirectory(tmpKafkaParent);
			}
			catch (Exception e) {
				// ignore
			}
		}
		if (tmpZkDir != null && tmpZkDir.exists()) {
			try {
				FileUtils.deleteDirectory(tmpZkDir);
			}
			catch (Exception e) {
				// ignore
			}
		}
	}

	public ZkUtils getZkUtils() {
		ZkClient creator = new ZkClient(zookeeperConnectionString, Integer.valueOf(standardProps.getProperty("zookeeper.session.timeout.ms")),
				Integer.valueOf(standardProps.getProperty("zookeeper.connection.timeout.ms")), new ZooKeeperStringSerializer());
		return ZkUtils.apply(creator, false);
	}

	public void createTestTopic(String topic, int numberOfPartitions, int replicationFactor, Properties topicConfig) {
		// create producerTopic with one client
		LOG.info("Creating producerTopic {}", topic);

		ZkUtils zkUtils = getZkUtils();
		try {
			AdminUtils.createTopic(zkUtils, topic, numberOfPartitions, replicationFactor, topicConfig, kafka.admin.RackAwareMode.Enforced$.MODULE$);
		} finally {
			zkUtils.close();
		}

		// validate that the producerTopic has been created
		final long deadline = System.nanoTime() + 30_000_000_000L;
		do {
			try {
				if (secureMode) {
					//increase wait time since in Travis ZK timeout occurs frequently
					int wait = zkTimeout / 100;
					LOG.info("waiting for {} msecs before the producerTopic {} can be checked", wait, topic);
					Thread.sleep(wait);
				} else {
					Thread.sleep(100);
				}
			} catch (InterruptedException e) {
				// restore interrupted state
			}
			// we could use AdminUtils.topicExists(zkUtils, producerTopic) here, but it's results are
			// not always correct.

			// create a new ZK utils connection
			ZkUtils checkZKConn = getZkUtils();
			if (AdminUtils.topicExists(checkZKConn, topic)) {
				checkZKConn.close();
				return;
			}
			checkZKConn.close();
		}
		while (System.nanoTime() < deadline);
		throw new IllegalStateException("Test producerTopic could not be created");
	}

	public void deleteTestTopic(String topic) {
		ZkUtils zkUtils = getZkUtils();
		try {
			LOG.info("Deleting producerTopic {}", topic);

			ZkClient zk = new ZkClient(zookeeperConnectionString, Integer.valueOf(standardProps.getProperty("zookeeper.session.timeout.ms")),
				Integer.valueOf(standardProps.getProperty("zookeeper.connection.timeout.ms")), new ZooKeeperStringSerializer());

			AdminUtils.deleteTopic(zkUtils, topic);

			zk.close();
		} finally {
			zkUtils.close();
		}
	}

	/**
	 * Copied from com.github.sakserv.minicluster.KafkaLocalBrokerIntegrationTest (ASL licensed).
	 */
	protected KafkaServer getKafkaServer(int brokerId, File tmpFolder) throws Exception {
		Properties kafkaProperties = new Properties();

		// properties have to be Strings
		kafkaProperties.put("advertised.host.name", KAFKA_HOST);
		kafkaProperties.put("broker.id", Integer.toString(brokerId));
		kafkaProperties.put("log.dir", tmpFolder.toString());
		kafkaProperties.put("zookeeper.connect", zookeeperConnectionString);
		kafkaProperties.put("message.max.bytes", String.valueOf(50 * 1024 * 1024));
		kafkaProperties.put("replica.fetch.max.bytes", String.valueOf(50 * 1024 * 1024));

		// for CI stability, increase zookeeper session timeout
		kafkaProperties.put("zookeeper.session.timeout.ms", zkTimeout);
		kafkaProperties.put("zookeeper.connection.timeout.ms", zkTimeout);
		if (additionalServerProperties != null) {
			kafkaProperties.putAll(additionalServerProperties);
		}

		final int numTries = 5;

		for (int i = 1; i <= numTries; i++) {
			int kafkaPort = NetUtils.getAvailablePort();
			kafkaProperties.put("port", Integer.toString(kafkaPort));

			//to support secure kafka cluster
			if (secureMode) {
				LOG.info("Adding Kafka secure configurations");
				kafkaProperties.put("listeners", "SASL_PLAINTEXT://" + KAFKA_HOST + ":" + kafkaPort);
				kafkaProperties.put("advertised.listeners", "SASL_PLAINTEXT://" + KAFKA_HOST + ":" + kafkaPort);
				kafkaProperties.putAll(getSecureProperties());
			}

			KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);

			try {
				scala.Option<String> stringNone = scala.Option.apply(null);
				KafkaServer server = new KafkaServer(kafkaConfig, Time.SYSTEM, stringNone, new ArraySeq<KafkaMetricsReporter>(0));
				server.startup();
				return server;
			}
			catch (KafkaException e) {
				if (e.getCause() instanceof BindException) {
					// port conflict, retry...
					LOG.info("Port conflict when starting Kafka Broker. Retrying...");
				}
				else {
					throw e;
				}
			}
		}

		throw new Exception("Could not start Kafka after " + numTries + " retries due to port conflicts.");
	}

	/**
	 * Simple ZooKeeper serializer for Strings.
	 */
	public static class ZooKeeperStringSerializer implements ZkSerializer {

		@Override
		public byte[] serialize(Object data) {
			if (data instanceof String) {
				return ((String) data).getBytes(ConfigConstants.DEFAULT_CHARSET);
			}
			else {
				throw new IllegalArgumentException("ZooKeeperStringSerializer can only serialize strings.");
			}
		}

		@Override
		public Object deserialize(byte[] bytes) {
			if (bytes == null) {
				return null;
			}
			else {
				return new String(bytes, ConfigConstants.DEFAULT_CHARSET);
			}
		}
	}
}
