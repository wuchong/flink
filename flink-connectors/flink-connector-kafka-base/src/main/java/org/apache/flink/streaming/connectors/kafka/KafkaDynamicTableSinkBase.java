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
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connectors.ChangelogMode;
import org.apache.flink.table.connectors.ChangelogSerializationSchema;
import org.apache.flink.table.connectors.DynamicTableSink;
import org.apache.flink.table.connectors.SinkFunctionChangelogWriter;
import org.apache.flink.table.connectors.SupportsChangelogWriting;
import org.apache.flink.table.dataformats.BaseRow;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

import java.util.Properties;

/**
 * A version-agnostic Kafka {@link AppendStreamTableSink}.
 *
 * <p>The version-specific Kafka consumers need to extend this class and
 * override {@link #createKafkaProducer(String, Properties, ChangelogSerializationSchema)}}.
 */
@Internal
public abstract class KafkaDynamicTableSinkBase implements DynamicTableSink, SupportsChangelogWriting {

	/** The schema of the table. */
	private final TableSchema schema;

	/** The Kafka topic to write to. */
	protected final String topic;

	/** Properties for the Kafka producer. */
	protected final Properties properties;

	/** Serialization schema for encoding records to Kafka. */
	protected final ChangelogSerializationSchema serializationSchema;

	protected KafkaDynamicTableSinkBase(
			TableSchema schema,
			String topic,
			Properties properties,
			ChangelogSerializationSchema serializationSchema) {
		this.schema = TableSchemaUtils.checkNoGeneratedColumns(schema);
		this.topic = Preconditions.checkNotNull(topic, "Topic must not be null.");
		this.properties = Preconditions.checkNotNull(properties, "Properties must not be null.");
		this.serializationSchema = Preconditions.checkNotNull(serializationSchema, "Serialization schema must not be null.");
	}

	@Override
	public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
		if (requestedMode.isInsertOnly()) {
			return requestedMode;
		} else {
			// contains changes
			// if serializationSchema only supports insert-only, the planner will check and throw exception
			// if serializationSchema returns an unsupported mode (e.g. insert+update_before), the planner will check and throw exception
			return serializationSchema.supportedChangelogMode();
		}
	}

	@Override
	public ChangelogWriter getChangelogWriter(ChangelogWriterContext context) {
		final SinkFunction<BaseRow> kafkaProducer = createKafkaProducer(
			topic,
			properties,
			serializationSchema);
		return SinkFunctionChangelogWriter.of(kafkaProducer);
	}

	/**
	 * Returns the version-specific Kafka producer.
	 *
	 * @param topic               Kafka topic to produce to.
	 * @param properties          Properties for the Kafka producer.
	 * @param serializationSchema Serialization schema to use to create Kafka records.
	 * @return The version-specific Kafka producer
	 */
	protected abstract SinkFunction<BaseRow> createKafkaProducer(
		String topic,
		Properties properties,
		ChangelogSerializationSchema serializationSchema);

	@Override
	public String asSummaryString() {
		return "KafkaSink";
	}
}
