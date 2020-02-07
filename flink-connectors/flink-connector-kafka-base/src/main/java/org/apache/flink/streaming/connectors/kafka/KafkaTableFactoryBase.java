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
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.KafkaValidator;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.factories.TableSinkV2Factory;
import org.apache.flink.table.factories.TableSourceV2Factory;
import org.apache.flink.table.sinks.v2.TableSinkV2;
import org.apache.flink.table.sources.v2.TableSourceV2;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_PROPERTIES;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SINK_PARTITIONER;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SINK_PARTITIONER_CLASS;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SPECIFIC_OFFSETS;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SPECIFIC_OFFSETS_OFFSET;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SPECIFIC_OFFSETS_PARTITION;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_STARTUP_MODE;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_STARTUP_TIMESTAMP_MILLIS;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_TOPIC;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_TYPE_VALUE_KAFKA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;

/**
 * Factory for creating configured instances of {@link KafkaTableSourceV2Base}.
 */
public abstract class KafkaTableFactoryBase implements
		TableSourceV2Factory,
		TableSinkV2Factory {

	@Override
	public String connectorType() {
		return CONNECTOR_TYPE_VALUE_KAFKA;
	}

	@Override
	public Optional<String> connectorVersion() {
		return Optional.of(kafkaVersion());
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();
		// kafka
		properties.add(CONNECTOR_TOPIC);
		properties.add(CONNECTOR_PROPERTIES);
		properties.add(CONNECTOR_PROPERTIES + ".*");
		properties.add(CONNECTOR_STARTUP_MODE);
		properties.add(CONNECTOR_SPECIFIC_OFFSETS);
		properties.add(CONNECTOR_STARTUP_TIMESTAMP_MILLIS);
		properties.add(CONNECTOR_SINK_PARTITIONER);
		properties.add(CONNECTOR_SINK_PARTITIONER_CLASS);

		// format wildcard
		properties.add(FORMAT + ".*");

		return properties;
	}

	@Override
	public TableSinkV2 createTableSink(TableSinkV2Factory.Context context) {
		Map<String, String> properties = context.getTable().toProperties();
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

		final TableSchema schema = TableSchemaUtils.getPhysicalSchema(
			descriptorProperties.getTableSchema(SCHEMA));
		final String topic = descriptorProperties.getString(CONNECTOR_TOPIC);

		return createKafkaTableSink(
			schema,
			topic,
			getKafkaProperties(descriptorProperties),
			getSerializationSchema(properties));
	}

	@Override
	public TableSourceV2 createTableSource(TableSourceV2Factory.Context context) {
		Map<String, String> properties = context.getTable().toProperties();
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

		final String topic = descriptorProperties.getString(CONNECTOR_TOPIC);
		final DeserializationSchema<?> deserializationSchema = getDeserializationSchema(properties);
		final StartupOptions startupOptions = getStartupOptions(descriptorProperties, topic);

		return createKafkaTableSource(
			TableSchemaUtils.getPhysicalSchema(descriptorProperties.getTableSchema(SCHEMA)),
			topic,
			getKafkaProperties(descriptorProperties),
			deserializationSchema,
			startupOptions.startupMode,
			startupOptions.specificOffsets,
			startupOptions.startupTimestampMillis);
	}

	// --------------------------------------------------------------------------------------------
	// For version-specific factories
	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the Kafka version.
	 */
	protected abstract String kafkaVersion();

	/**
	 * True if the Kafka source supports Kafka timestamps, false otherwise.
	 *
	 * @return True if the Kafka source supports Kafka timestamps, false otherwise.
	 */
	protected abstract boolean supportsKafkaTimestamps();

	/**
	 * Constructs the version-specific Kafka table source.
	 *
	 * @param schema                      Schema of the produced table.
	 *                                    fields of the physical returned type.
	 * @param topic                       Kafka topic to consume.
	 * @param properties                  Properties for the Kafka consumer.
	 * @param deserializationSchema       Deserialization schema for decoding records from Kafka.
	 * @param startupMode                 Startup mode for the contained consumer.
	 * @param specificStartupOffsets      Specific startup offsets; only relevant when startup
	 *                                    mode is {@link StartupMode#SPECIFIC_OFFSETS}.
	 */
	protected abstract KafkaTableSourceV2Base createKafkaTableSource(
		TableSchema schema,
		String topic,
		Properties properties,
		DeserializationSchema<?> deserializationSchema,
		StartupMode startupMode,
		Map<KafkaTopicPartition, Long> specificStartupOffsets,
		long startupTimestampMillis);

	/**
	 * Constructs the version-specific Kafka table sink.
	 *
	 * @param schema      Schema of the produced table.
	 * @param topic       Kafka topic to consume.
	 * @param properties  Properties for the Kafka consumer.
	 */
	protected abstract TableSinkV2 createKafkaTableSink(
		TableSchema schema,
		String topic,
		Properties properties,
		SerializationSchema<?> serializationSchema);

	// --------------------------------------------------------------------------------------------
	// Helper methods
	// --------------------------------------------------------------------------------------------

	private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);

		// allow Kafka timestamps to be used, watermarks can not be received from source
		new SchemaValidator(true, supportsKafkaTimestamps(), false).validate(descriptorProperties);
		new KafkaValidator().validate(descriptorProperties);

		return descriptorProperties;
	}

	private DeserializationSchema<?> getDeserializationSchema(Map<String, String> properties) {
		final DeserializationSchemaFactory<?> formatFactory = TableFactoryService.find(
			DeserializationSchemaFactory.class,
			properties,
			this.getClass().getClassLoader());
		return formatFactory.createDeserializationSchema(properties);
	}

	private SerializationSchema<?> getSerializationSchema(Map<String, String> properties) {
		final SerializationSchemaFactory<?> formatFactory = TableFactoryService.find(
			SerializationSchemaFactory.class,
			properties,
			this.getClass().getClassLoader());
		return formatFactory.createSerializationSchema(properties);
	}

	private Properties getKafkaProperties(DescriptorProperties descriptorProperties) {
		final Properties kafkaProperties = new Properties();

		descriptorProperties.asMap().keySet()
			.stream()
			.filter(key -> key.startsWith(CONNECTOR_PROPERTIES))
			.forEach(key -> {
				final String value = descriptorProperties.getString(key);
				final String subKey = key.substring((CONNECTOR_PROPERTIES + '.').length());
				kafkaProperties.put(subKey, value);
			});

		return kafkaProperties;
	}

	private StartupOptions getStartupOptions(
			DescriptorProperties descriptorProperties,
			String topic) {
		final Map<KafkaTopicPartition, Long> specificOffsets = new HashMap<>();
		final StartupMode startupMode = descriptorProperties
			.getOptionalString(CONNECTOR_STARTUP_MODE)
			.map(modeString -> {
				switch (modeString) {
					case KafkaValidator.CONNECTOR_STARTUP_MODE_VALUE_EARLIEST:
						return StartupMode.EARLIEST;

					case KafkaValidator.CONNECTOR_STARTUP_MODE_VALUE_LATEST:
						return StartupMode.LATEST;

					case KafkaValidator.CONNECTOR_STARTUP_MODE_VALUE_GROUP_OFFSETS:
						return StartupMode.GROUP_OFFSETS;

					case KafkaValidator.CONNECTOR_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS:
						buildSpecificOffsets(descriptorProperties, topic, specificOffsets);
						return StartupMode.SPECIFIC_OFFSETS;

					case KafkaValidator.CONNECTOR_STARTUP_MODE_VALUE_TIMESTAMP:
						return StartupMode.TIMESTAMP;

					default:
						throw new TableException("Unsupported startup mode. Validator should have checked that.");
				}
			}).orElse(StartupMode.GROUP_OFFSETS);
		final StartupOptions options = new StartupOptions();
		options.startupMode = startupMode;
		options.specificOffsets = specificOffsets;
		if (startupMode == StartupMode.TIMESTAMP) {
			options.startupTimestampMillis = descriptorProperties.getLong(CONNECTOR_STARTUP_TIMESTAMP_MILLIS);
		}
		return options;
	}

	private void buildSpecificOffsets(DescriptorProperties descriptorProperties, String topic, Map<KafkaTopicPartition, Long> specificOffsets) {
		if (descriptorProperties.containsKey(CONNECTOR_SPECIFIC_OFFSETS)) {
			final Map<Integer, Long> offsetMap = KafkaValidator.validateAndParseSpecificOffsetsString(descriptorProperties);
			offsetMap.forEach((partition, offset) -> {
				final KafkaTopicPartition topicPartition = new KafkaTopicPartition(topic, partition);
				specificOffsets.put(topicPartition, offset);
			});
		} else {
			final List<Map<String, String>> offsetList = descriptorProperties.getFixedIndexedProperties(
					CONNECTOR_SPECIFIC_OFFSETS,
					Arrays.asList(CONNECTOR_SPECIFIC_OFFSETS_PARTITION, CONNECTOR_SPECIFIC_OFFSETS_OFFSET));
			offsetList.forEach(kv -> {
				final int partition = descriptorProperties.getInt(kv.get(CONNECTOR_SPECIFIC_OFFSETS_PARTITION));
				final long offset = descriptorProperties.getLong(kv.get(CONNECTOR_SPECIFIC_OFFSETS_OFFSET));
				final KafkaTopicPartition topicPartition = new KafkaTopicPartition(topic, partition);
				specificOffsets.put(topicPartition, offset);
			});
		}
	}

	private static class StartupOptions {
		private StartupMode startupMode;
		private Map<KafkaTopicPartition, Long> specificOffsets;
		private long startupTimestampMillis;
	}
}
