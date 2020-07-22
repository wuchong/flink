/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.descriptor3;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import java.util.Map;
import java.util.Properties;

import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

/**
 * Connector descriptor for the Apache Kafka message queue.
 */
@PublicEvolving
public class KafkaConnector extends TableDescriptor {

	public static KafkaConnectorBuilder newBuilder() {
		return new KafkaConnectorBuilder();
	}

	public static class KafkaConnectorBuilder extends TableDescriptorBuilder<KafkaConnector, KafkaConnectorBuilder> {

		public KafkaConnectorBuilder() {
			super(KafkaConnector.class);
		}

		/**
		 * Sets the Kafka version to be used.
		 *
		 * @param version Kafka version. E.g., "0.8", "0.11", etc.
		 */
		public KafkaConnectorBuilder version(String version) {
			Preconditions.checkNotNull(version);
			option(CONNECTOR.key(), "kafka-" + version);
			return this;
		}

		/**
		 * Sets the topic from which the table is read.
		 *
		 * @param topic The topic from which the table is read.
		 */
		public KafkaConnectorBuilder topic(String topic) {
			Preconditions.checkNotNull(topic);
			option("topic", topic);
			return this;
		}

		/**
		 * Sets the configuration properties for the Kafka consumer. Resets previously set properties.
		 *
		 * @param properties The configuration properties for the Kafka consumer.
		 */
		public KafkaConnectorBuilder properties(Properties properties) {
			Preconditions.checkNotNull(properties);
			for (Map.Entry<Object, Object> entry : properties.entrySet()) {
				property(entry.getKey().toString(), entry.getValue().toString());
			}
			return this;
		}

		/**
		 * Adds a configuration properties for the Kafka consumer.
		 *
		 * @param key   property key for the Kafka consumer
		 * @param value property value for the Kafka consumer
		 */
		public KafkaConnectorBuilder property(String key, String value) {
			Preconditions.checkNotNull(key);
			Preconditions.checkNotNull(value);
			option("properties." + key, value);
			return this;
		}

		/**
		 * Configures to start reading from the earliest offset for all partitions.
		 */
		public KafkaConnectorBuilder startFromEarliest() {
			// TODO
			return this;
		}

		/**
		 * Configures to start reading from the latest offset for all partitions.
		 */
		public KafkaConnectorBuilder startFromLatest() {
			// TODO
			return this;
		}

		/**
		 * Configures to start reading from any committed group offsets found in Zookeeper / Kafka brokers.
		 */
		public KafkaConnectorBuilder startFromGroupOffsets() {
			// TODO
			return this;
		}

		/**
		 * Configures to start reading partitions from specific offsets, set independently for each partition.
		 * Resets previously set offsets.
		 *
		 * @param specificOffsets the specified offsets for partitions
		 */
		public KafkaConnectorBuilder startFromSpecificOffsets(Map<Integer, Long> specificOffsets) {
			// TODO
			return this;
		}

		/**
		 * Configures to start reading partitions from specific offsets and specifies the given offset for
		 * the given partition.
		 *
		 * @param partition      partition index
		 * @param specificOffset partition offset to start reading from
		 */
		public KafkaConnectorBuilder startFromSpecificOffset(int partition, long specificOffset) {
			// TODO
			return this;
		}

		/**
		 * Configures to start reading from partition offsets of the specified timestamp.
		 *
		 * @param startTimestampMillis timestamp to start reading from
		 */
		public KafkaConnectorBuilder startFromTimestamp(long startTimestampMillis) {
			// TODO
			return this;
		}

		/**
		 * Configures how to partition records from Flink's partitions into Kafka's partitions.
		 *
		 * <p>This strategy ensures that each Flink partition ends up in one Kafka partition.
		 *
		 * <p>Note: One Kafka partition can contain multiple Flink partitions. Examples:
		 *
		 * <p>More Flink partitions than Kafka partitions. Some (or all) Kafka partitions contain
		 * the output of more than one flink partition:
		 * <pre>
		 *     Flink Sinks            Kafka Partitions
		 *         1    ----------------&gt;    1
		 *         2    --------------/
		 *         3    -------------/
		 *         4    ------------/
		 * </pre>
		 *
		 *
		 * <p>Fewer Flink partitions than Kafka partitions:
		 * <pre>
		 *     Flink Sinks            Kafka Partitions
		 *         1    ----------------&gt;    1
		 *         2    ----------------&gt;    2
		 *                                      3
		 *                                      4
		 *                                      5
		 * </pre>
		 */
		public KafkaConnectorBuilder sinkPartitionerFixed() {
			// TODO
			return this;
		}

		/**
		 * Configures how to partition records from Flink's partitions into Kafka's partitions.
		 *
		 * <p>This strategy ensures that records will be distributed to Kafka partitions in a
		 * round-robin fashion.
		 *
		 * <p>Note: This strategy is useful to avoid an unbalanced partitioning. However, it will
		 * cause a lot of network connections between all the Flink instances and all the Kafka brokers.
		 */
		public KafkaConnectorBuilder sinkPartitionerRoundRobin() {
			// TODO
			return this;
		}

		public KafkaConnectorBuilder format(FormatDescriptor formatDescriptor) {
			// TODO
			return this;
		}

		@Override
		protected KafkaConnectorBuilder self() {
			return this;
		}
	}
}
