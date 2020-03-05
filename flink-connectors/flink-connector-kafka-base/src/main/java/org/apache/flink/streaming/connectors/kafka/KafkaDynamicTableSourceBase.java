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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connectors.ChangelogDeserializationSchema;
import org.apache.flink.table.connectors.ChangelogMode;
import org.apache.flink.table.connectors.DynamicTableSource;
import org.apache.flink.table.connectors.SourceFunctionReader;
import org.apache.flink.table.connectors.SupportsChangelogReading;
import org.apache.flink.table.dataformats.BaseRow;

import java.util.Map;
import java.util.Properties;

@Internal
public abstract class KafkaDynamicTableSourceBase implements DynamicTableSource, SupportsChangelogReading {
	// common table source attributes

	/** The schema of the table. */
	private final TableSchema schema;

	// Kafka-specific attributes

	/** The Kafka topic to consume. */
	private final String topic;

	/** Properties for the Kafka consumer. */
	private final Properties properties;

	/** Deserialization schema for decoding records from Kafka. */
	private final ChangelogDeserializationSchema deserializationSchema;

	/** The startup mode for the contained consumer (default is {@link StartupMode#GROUP_OFFSETS}). */
	private final StartupMode startupMode;

	/** Specific startup offsets; only relevant when startup mode is {@link StartupMode#SPECIFIC_OFFSETS}. */
	private final Map<KafkaTopicPartition, Long> specificStartupOffsets;

	/** The start timestamp to locate partition offsets; only relevant when startup mode is {@link StartupMode#TIMESTAMP}.*/
	private final long startupTimestampMillis;

	protected KafkaDynamicTableSourceBase(TableSchema schema, String topic, Properties properties, ChangelogDeserializationSchema deserializationSchema, StartupMode startupMode, Map<KafkaTopicPartition, Long> specificStartupOffsets, long startupTimestampMillis) {
		this.schema = schema;
		this.topic = topic;
		this.properties = properties;
		this.deserializationSchema = deserializationSchema;
		this.startupMode = startupMode;
		this.specificStartupOffsets = specificStartupOffsets;
		this.startupTimestampMillis = startupTimestampMillis;
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return deserializationSchema.producedChangelogMode();
	}

	@Override
	public ChangelogReader getChangelogReader(Context context) {
		// Version-specific Kafka consumer
		FlinkKafkaConsumerBase<BaseRow> kafkaConsumer = getKafkaConsumer(
			topic,
			properties,
			deserializationSchema);
		return SourceFunctionReader.of(kafkaConsumer, false);
	}

	@Override
	public String asSummaryString() {
		return null;
	}

	/**
	 * Returns the properties for the Kafka consumer.
	 *
	 * @return properties for the Kafka consumer.
	 */
	public Properties getProperties() {
		return properties;
	}

	/**
	 * Returns the deserialization schema.
	 *
	 * @return The deserialization schema
	 */
	public DeserializationSchema<?> getDeserializationSchema(){
		return deserializationSchema;
	}

	/**
	 * Returns a version-specific Kafka consumer with the start position configured.
	 *
	 * @param topic                 Kafka topic to consume.
	 * @param properties            Properties for the Kafka consumer.
	 * @param deserializationSchema Deserialization schema to use for Kafka records.
	 * @return The version-specific Kafka consumer
	 */
	protected FlinkKafkaConsumerBase<BaseRow> getKafkaConsumer(
		String topic,
		Properties properties,
		ChangelogDeserializationSchema deserializationSchema) {
		FlinkKafkaConsumerBase<BaseRow> kafkaConsumer =
			createKafkaConsumer(topic, properties, deserializationSchema);
		switch (startupMode) {
			case EARLIEST:
				kafkaConsumer.setStartFromEarliest();
				break;
			case LATEST:
				kafkaConsumer.setStartFromLatest();
				break;
			case GROUP_OFFSETS:
				kafkaConsumer.setStartFromGroupOffsets();
				break;
			case SPECIFIC_OFFSETS:
				kafkaConsumer.setStartFromSpecificOffsets(specificStartupOffsets);
				break;
			case TIMESTAMP:
				kafkaConsumer.setStartFromTimestamp(startupTimestampMillis);
				break;
		}
		return kafkaConsumer;
	}

	//////// ABSTRACT METHODS FOR SUBCLASSES

	/**
	 * Creates a version-specific Kafka consumer.
	 *
	 * @param topic                 Kafka topic to consume.
	 * @param properties            Properties for the Kafka consumer.
	 * @param deserializationSchema Deserialization schema to use for Kafka records.
	 * @return The version-specific Kafka consumer
	 */
	protected abstract FlinkKafkaConsumerBase<BaseRow> createKafkaConsumer(
		String topic,
		Properties properties,
		ChangelogDeserializationSchema deserializationSchema);
}
