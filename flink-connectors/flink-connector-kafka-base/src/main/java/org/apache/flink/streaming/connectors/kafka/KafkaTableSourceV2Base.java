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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.dataformat.ChangeRow;
import org.apache.flink.table.sources.v2.ChangeMode;
import org.apache.flink.table.sources.v2.DataReaderProvider;
import org.apache.flink.table.sources.v2.DynamicTableType;
import org.apache.flink.table.sources.v2.SourceFunctionProvider;
import org.apache.flink.table.sources.v2.TableSourceV2;
import org.apache.flink.table.sources.v2.UpdateMode;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.typeutils.ChangeRowTypeInfo;

import java.util.Map;
import java.util.Properties;

/**
 * .
 */
public abstract class KafkaTableSourceV2Base implements TableSourceV2 {

	// common table source attributes

	/** The schema of the table. */
	private final TableSchema schema;

	// Kafka-specific attributes

	/** The Kafka topic to consume. */
	private final String topic;

	/** Properties for the Kafka consumer. */
	private final Properties properties;

	/** Deserialization schema for decoding records from Kafka. */
	private final DeserializationSchema<?> deserializationSchema;

	/** The startup mode for the contained consumer (default is {@link StartupMode#GROUP_OFFSETS}). */
	private final StartupMode startupMode;

	/** Specific startup offsets; only relevant when startup mode is {@link StartupMode#SPECIFIC_OFFSETS}. */
	private final Map<KafkaTopicPartition, Long> specificStartupOffsets;

	/** The start timestamp to locate partition offsets; only relevant when startup mode is {@link StartupMode#TIMESTAMP}.*/
	private final long startupTimestampMillis;

	private final boolean isAppendOnly;

	/** The default value when startup timestamp is not used.*/
	private static final long DEFAULT_STARTUP_TIMESTAMP_MILLIS = 0L;

	protected KafkaTableSourceV2Base(TableSchema schema, String topic, Properties properties, DeserializationSchema<?> deserializationSchema, StartupMode startupMode, Map<KafkaTopicPartition, Long> specificStartupOffsets, long startupTimestampMillis) {
		this.schema = schema;
		this.topic = topic;
		this.properties = properties;
		this.deserializationSchema = deserializationSchema;
		this.startupMode = startupMode;
		this.specificStartupOffsets = specificStartupOffsets;
		this.startupTimestampMillis = startupTimestampMillis;
		if (deserializationSchema.getProducedType() instanceof RowTypeInfo) {
			this.isAppendOnly = true;
		} else if (deserializationSchema.getProducedType() instanceof ChangeRowTypeInfo) {
			this.isAppendOnly = false;
		} else {
			throw new IllegalArgumentException(
				"The return type of DeserializationSchema of Kafka can only be Row type or ChangeRow type.");
		}
	}

	@Override
	public DynamicTableType getDynamicTableType() {
		return DynamicTableType.STREAM;
	}

	@Override
	public boolean isBounded() {
		return false;
	}

	@Override
	public DataType getProduceDataType() {
		if (isAppendOnly) {
			return schema.toRowDataType();
		} else {
			return schema.toRowDataType().bridgedTo(ChangeRow.class);
		}
	}

	@Override
	public ChangeMode getChangeMode() {
		if (isAppendOnly) {
			return ChangeMode.INSERT_ONLY;
		} else {
			return ChangeMode.IGNORE_UPDATE_OLD;
		}
	}

	@Override
	public DataReaderProvider<?> createDataReaderProvider() {
		DeserializationSchema<?> deserializationSchema = getDeserializationSchema();
		// Version-specific Kafka consumer
		FlinkKafkaConsumerBase<?> kafkaConsumer = getKafkaConsumer(
			topic,
			properties,
			deserializationSchema);
		return SourceFunctionProvider.of(kafkaConsumer);
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
	protected FlinkKafkaConsumerBase<?> getKafkaConsumer(
			String topic,
			Properties properties,
			DeserializationSchema<?> deserializationSchema) {
		FlinkKafkaConsumerBase<?> kafkaConsumer =
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
	protected abstract FlinkKafkaConsumerBase<?> createKafkaConsumer(
		String topic,
		Properties properties,
		DeserializationSchema<?> deserializationSchema);
}
