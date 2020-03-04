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
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connectors.ChangelogDeserializationSchema;
import org.apache.flink.table.descriptors.KafkaValidator;

import java.util.Map;
import java.util.Properties;

public class KafkaDynamicTableFactory extends KafkaDynamicTableFactoryBase {
	@Override
	protected String kafkaVersion() {
		return KafkaValidator.CONNECTOR_VERSION_VALUE_UNIVERSAL;
	}


	@Override
	protected boolean supportsKafkaTimestamps() {
		return true;
	}

	@Override
	protected KafkaDynamicTableSourceBase createKafkaTableSource(
			TableSchema schema,
			String topic,
			Properties properties,
			ChangelogDeserializationSchema deserializationSchema,
			StartupMode startupMode,
			Map<KafkaTopicPartition, Long> specificStartupOffsets,
			long startupTimestampMillis) {
		return new KafkaDynamicTableSource(schema, topic, properties, deserializationSchema, startupMode, specificStartupOffsets, startupTimestampMillis);
	}
}
