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

import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connectors.ChangelogDeserializationSchema;
import org.apache.flink.table.connectors.DynamicTableSource;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.KafkaValidator;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.ChangelogDeserializationSchemaFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;
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

public abstract class KafkaDynamicTableFactoryBase implements DynamicTableSourceFactory {

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_KAFKA); // kafka
		context.put(CONNECTOR_VERSION, kafkaVersion()); // version
		context.put(CONNECTOR_PROPERTY_VERSION, "1"); // backwards compatibility
		return context;
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
	public DynamicTableSource createTableSource(Context context) {
		Map<String, String> properties = context.getTable().toProperties();
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

		final String topic = descriptorProperties.getString(CONNECTOR_TOPIC);
		final ChangelogDeserializationSchema deserializationSchema = getDeserializationSchema(properties);
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
	protected abstract KafkaDynamicTableSourceBase createKafkaTableSource(
		TableSchema schema,
		String topic,
		Properties properties,
		ChangelogDeserializationSchema deserializationSchema,
		StartupMode startupMode,
		Map<KafkaTopicPartition, Long> specificStartupOffsets,
		long startupTimestampMillis);

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

	private ChangelogDeserializationSchema getDeserializationSchema(Map<String, String> properties) {
		final ChangelogDeserializationSchemaFactory formatFactory = TableFactoryService.find(
			ChangelogDeserializationSchemaFactory.class,
			properties,
			this.getClass().getClassLoader());
		return formatFactory.createDeserializationSchema(properties);
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
