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

package org.apache.flink.table.api;

import org.apache.flink.table.descriptor.Connector;
import org.apache.flink.table.descriptor.Json;
import org.apache.flink.table.descriptor.Kafka;
import org.apache.flink.table.descriptor.Schema;
import org.apache.flink.table.descriptor.TableDescriptor;

import org.junit.Test;

import java.time.Duration;

public class TableDescriptorTest {

	TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());

	@Test
	public void testV1() {
		tEnv.createTemporaryTable(
			"MyTable",
			new TableDescriptor()
				.connector(
					new Kafka()
						.version("0.11")
						.topic("user_logs")
						.property("bootstrap.servers", "localhost:9092")
						.property("group.id", "test-group")
						.startFromEarliest()
						.sinkPartitionerRoundRobin()
						.format(new Json().ignoreParseErrors(false)))
				.schema(
					new Schema()
						.column("user_id", DataTypes.BIGINT())
						.column("user_name", DataTypes.STRING())
						.column("score", DataTypes.DECIMAL(10, 2))
						.column("log_ts", DataTypes.STRING())
						.column("part_field_0", DataTypes.STRING())
						.column("part_field_1", DataTypes.INT())
						.proctime("proc")
						.computedColumn("my_ts", "TO_TIMESTAMP(log_ts)")  // 计算列
						.watermarkFor("my_ts").boundedOutOfOrderTimestamps(Duration.ofSeconds(5))
						.primaryKey("user_id"))
				.partitionedBy("part_field_0", "part_field_1")
		);

		tEnv.createTemporaryTable(
			"MyTable",
			new TableDescriptor()
				.connector(
					new Connector("kafka-0.11")
						.option("topic", "user_logs")
						.option("properties.bootstrap.servers", "localhost:9092")
						.option("properties.group.id", "test-group")
						.option("scan.startup.mode", "earliest")
						.option("format", "json")
						.option("json.ignore-parse-errors", "true")
						.option("sink.partitioner", "round-robin"))
				.schema(
					new Schema()
						.column("user_id", DataTypes.BIGINT())
						.column("user_name", DataTypes.STRING())
						.column("score", DataTypes.DECIMAL(10, 2))
						.column("log_ts", DataTypes.STRING())
						.column("part_field_0", DataTypes.STRING())
						.column("part_field_1", DataTypes.INT())
						.proctime("proc")
						.computedColumn("my_ts", "TO_TIMESTAMP(log_ts)")  // 计算列
						.watermarkFor("my_ts").boundedOutOfOrderTimestamps(Duration.ofSeconds(5))
						.primaryKey("user_id"))
				.partitionedBy("part_field_0", "part_field_1")
		);
	}

	@Test
	public void testV2() {
		tEnv.connect(
				new Kafka()
					.version("0.11")
					.topic("myTopic")
					.property("bootstrap.servers", "localhost:9092")
					.property("group.id", "test-group")
					.startFromEarliest()
					.sinkPartitionerRoundRobin()
					.format(new Json().ignoreParseErrors(false)))
			.schema(
				new Schema()
					.column("user_id", DataTypes.BIGINT())
					.column("user_name", DataTypes.STRING())
					.column("score", DataTypes.DECIMAL(10, 2))
					.column("log_ts", DataTypes.TIMESTAMP(3))
					.column("part_field_0", DataTypes.STRING())
					.column("part_field_1", DataTypes.INT())
					.proctime("proc")
					.computedColumn("my_ts", "TO_TIMESTAMP(log_ts)")  // 计算列
					.watermarkFor("my_ts").boundedOutOfOrderTimestamps(Duration.ofSeconds(5))
					.primaryKey("user_id"))
			.partitionedBy("part_field_0", "part_field_1")
			// can still apply `.option(key, value)` if the option is not supported in Kafka descriptor
			.createTemporaryTable("MyTable");

		tEnv.connect(
				new Connector("kafka-0.11")
					.option("topic", "user_logs")
					.option("properties.bootstrap.servers", "localhost:9092")
					.option("properties.group.id", "test-group")
					.option("scan.startup.mode", "earliest")
					.option("format", "json")
					.option("json.ignore-parse-errors", "true")
					.option("sink.partitioner", "round-robin"))
			.schema(
				new Schema()
					.column("user_id", DataTypes.BIGINT())
					.column("user_name", DataTypes.STRING())
					.column("score", DataTypes.DECIMAL(10, 2))
					.column("log_ts", DataTypes.TIMESTAMP(3))
					.column("part_field_0", DataTypes.STRING())
					.column("part_field_1", DataTypes.INT())
					.proctime("proc")
					.computedColumn("my_ts", "TO_TIMESTAMP(log_ts)")  // 计算列
					.watermarkFor("my_ts").boundedOutOfOrderTimestamps(Duration.ofSeconds(5))
					.primaryKey("user_id"))
			.partitionedBy("part_field_0", "part_field_1")
			// can still apply `.option(key, value)` if the option is not supported in Kafka descriptor
			.createTemporaryTable("MyTable");
	}
}
