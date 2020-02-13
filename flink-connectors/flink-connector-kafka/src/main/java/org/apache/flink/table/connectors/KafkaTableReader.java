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

package org.apache.flink.table.connectors;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.Properties;

public class KafkaTableReader implements SourceFunctionTableReader {

	private final Properties properties;

	private final String topic;

	private final DataType producedRowType;

	public KafkaTableReader(Properties properties, String topic, DataType producedRowType) {
		this.properties = properties;
		this.topic = topic;
		this.producedRowType = producedRowType;
	}

	@Override
	public SourceFunction<ChangelogRow> createSourceFunction(Context context, WatermarkAssigner assigner) {
		final DeserializationSchema<Row> customSchema = getCustomSchemaThatUsesDefaultConversionClasses();

		final DataFormatConverter internalConverter = context.createDataFormatConverter(producedRowType);

		final TypeInformation<ChangelogRow> internalTypeInfo = context.createRowTypeInformation(producedRowType);

		final DeserializationSchema<ChangelogRow> internalSchema = new SimpleDeserializationSchema(
			customSchema,
			internalConverter,
			internalTypeInfo);

		final FlinkKafkaConsumer<ChangelogRow> sourceFunction = new FlinkKafkaConsumer<>(topic, internalSchema, properties);
		assigner.getPeriodicAssigner().ifPresent(sourceFunction::assignTimestampsAndWatermarks);
		assigner.getPunctuatedAssigner().ifPresent(sourceFunction::assignTimestampsAndWatermarks);
		return sourceFunction;
	}

	@Override
	public boolean isBounded() {
		return false;
	}

	@Override
	public ChangeMode getChangeMode() {
		return ChangeMode.INSERT_ONLY;
	}

	@Override
	public String asSummaryString() {
		return "Kafka Source";
	}

	private DeserializationSchema<Row> getCustomSchemaThatUsesDefaultConversionClasses() {
		// assuming we have a schema that has already the correct conversion classes
		return null;
	}

	// --------------------------------------------------------------------------------------------

	public static class SimpleDeserializationSchema implements DeserializationSchema<ChangelogRow> {

		private final DeserializationSchema<Row> customSchema;

		private final DataFormatConverter converter;

		private final TypeInformation<ChangelogRow> internalTypeInfo;

		public SimpleDeserializationSchema(
				DeserializationSchema<Row> customSchema,
				DataFormatConverter converter,
				TypeInformation<ChangelogRow> internalTypeInfo) {
			this.customSchema = customSchema;
			this.converter = converter;
			this.internalTypeInfo = internalTypeInfo;
		}

		// TODO not existing yet
		public void open() {
			this.converter.open(FormatConverter.Context.empty());
		}

		@Override
		public ChangelogRow deserialize(byte[] message) throws IOException {
			final Row row = customSchema.deserialize(message);
			return converter.toInternalRow(ChangelogRow.Kind.INSERT, row);
		}

		@Override
		public boolean isEndOfStream(ChangelogRow nextElement) {
			return false;
		}

		@Override
		public TypeInformation<ChangelogRow> getProducedType() {
			return internalTypeInfo;
		}
	}

}
