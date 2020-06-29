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

package org.apache.flink.table.descriptor;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

/**
 * Connector descriptor for the Apache Kafka message queue.
 */
@PublicEvolving
public class Kafka extends ConnectorDescriptor {

	private final Map<String, String> options = new HashMap<>();

	/**
	 * Connector descriptor for the Apache Kafka message queue.
	 */
	public Kafka() {
		this.options.put(CONNECTOR.key(), "kafka");
	}

	/**
	 * Sets the Kafka version to be used.
	 *
	 * @param version Kafka version. E.g., "0.8", "0.11", etc.
	 */
	public Kafka version(String version) {
		Preconditions.checkNotNull(version);
		this.options.put(CONNECTOR.key(), "kafka-" + version);
		return this;
	}

	/**
	 * Sets the topic from which the table is read.
	 *
	 * @param topic The topic from which the table is read.
	 */
	public Kafka topic(String topic) {
		Preconditions.checkNotNull(topic);
		return this;
	}

	/**
	 * Sets the configuration properties for the Kafka consumer. Resets previously set properties.
	 *
	 * @param properties The configuration properties for the Kafka consumer.
	 */
	public Kafka properties(Properties properties) {
		Preconditions.checkNotNull(properties);
		return this;
	}

	/**
	 * Adds a configuration properties for the Kafka consumer.
	 *
	 * @param key property key for the Kafka consumer
	 * @param value property value for the Kafka consumer
	 */
	public Kafka property(String key, String value) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(value);
		return this;
	}

	/**
	 * Configures to start reading from the earliest offset for all partitions.
	 *
	 */
	public Kafka startFromEarliest() {
		return this;
	}

	/**
	 * Configures to start reading from the latest offset for all partitions.
	 *
	 */
	public Kafka startFromLatest() {
		return this;
	}

	/**
	 * Configures to start reading from any committed group offsets found in Zookeeper / Kafka brokers.
	 *
	 */
	public Kafka startFromGroupOffsets() {
		return this;
	}

	/**
	 * Configures to start reading partitions from specific offsets, set independently for each partition.
	 * Resets previously set offsets.
	 *
	 * @param specificOffsets the specified offsets for partitions
	 */
	public Kafka startFromSpecificOffsets(Map<Integer, Long> specificOffsets) {
		return this;
	}

	/**
	 * Configures to start reading partitions from specific offsets and specifies the given offset for
	 * the given partition.
	 *
	 * @param partition partition index
	 * @param specificOffset partition offset to start reading from
	 */
	public Kafka startFromSpecificOffset(int partition, long specificOffset) {
		return this;
	}

	/**
	 * Configures to start reading from partition offsets of the specified timestamp.
	 *
	 * @param startTimestampMillis timestamp to start reading from
	 */
	public Kafka startFromTimestamp(long startTimestampMillis) {
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
	 *
	 */
	public Kafka sinkPartitionerFixed() {
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
	public Kafka sinkPartitionerRoundRobin() {
		return this;
	}

	public Kafka format(FormatDescriptor formatDescriptor) {
		return this;
	}

	@Override
	protected Map<String, String> toConnectorOptions() {
		return null;
	}
}
