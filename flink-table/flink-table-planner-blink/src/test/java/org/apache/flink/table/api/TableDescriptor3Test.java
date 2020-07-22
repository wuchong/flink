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

import org.apache.flink.table.descriptor3.Connector;
import org.apache.flink.table.descriptor3.JsonFormat;
import org.apache.flink.table.descriptor3.KafkaConnector;
import org.apache.flink.table.descriptor3.LikeOption;
import org.apache.flink.table.descriptor3.Schema;

import org.junit.Test;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;
import static org.apache.flink.table.api.Expressions.proctime;

public class TableDescriptor3Test {

	TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());

	@Test
	public void testLike() {
		tEnv.createTemporaryTable(
			"OrdersInKafka",
			KafkaConnector.newBuilder()
				.topic("user_logs")
				.property("bootstrap.servers", "localhost:9092")
				.property("group.id", "test-group")
				.format(JsonFormat.newInstance())
				.schema(
					Schema.newBuilder()
						.column("user_id", DataTypes.BIGINT())
						.column("score", DataTypes.DECIMAL(10, 2))
						.column("log_ts", DataTypes.TIMESTAMP(3))
						.column("ts", $("log_ts"))
						.build())
				.build()
		);

		tEnv.createTemporaryTable(
			"OrdersInFilesystem",
			Connector.of("filesystem")
				.option("path", "path/to/whatever")
				.schema(
					Schema.newBuilder()
						.watermark("ts", $("ts").minus(lit(3).seconds()))
						.build())
				.like("OrdersInKafka", LikeOption.EXCLUDING.ALL, LikeOption.INCLUDING.GENERATED)
				.build()
		);

		tEnv.createTemporaryTable(
			"OrdersInFilesystem",
			Connector.of("filesystem")
				.option("path", "path/to/whatever")
				.schema(
					Schema.newBuilder()
						.column("user_id", DataTypes.BIGINT())
						.column("score", DataTypes.DECIMAL(10, 2))
						.column("log_ts", DataTypes.TIMESTAMP(3))
						.column("my_ts", $("ts"))
						.build())
				.build()
		);
	}

	@Test
	public void testV3() {
		// register a table using specific descriptor
		tEnv.createTemporaryTable(
			"MyTable",
			KafkaConnector.newBuilder()
				.version("0.11")
				.topic("user_logs")
				.property("bootstrap.servers", "localhost:9092")
				.property("group.id", "test-group")
				.startFromEarliest()
				.sinkPartitionerRoundRobin()
				.format(JsonFormat.newBuilder().ignoreParseErrors(false).build())
				.schema(
					Schema.newBuilder()
						.column("user_id", DataTypes.BIGINT())
						.column("user_name", DataTypes.STRING())
						.column("score", DataTypes.DECIMAL(10, 2))
						.column("log_ts", DataTypes.STRING())
						.column("part_field_0", DataTypes.STRING())
						.column("part_field_1", DataTypes.INT())
						.column("proc", proctime()) // define a processing-time attribute with column name "proc"
						.column("ts", $("log_ts"))
						.watermark("ts", $("ts").minus(lit(3).seconds()))
						.primaryKey("user_id")
						.build())
				.partitionedBy("part_field_0", "part_field_1")  // Kafka doesn't support partitioned table yet, this is just an example for the API
				.build()
		);

		// register a table using general purpose Connector descriptor, this would be helpful for custom source/sinks
		tEnv.createTemporaryTable(
			"MyTable",
			Connector.of("kafka-0.11")
				.option("topic", "user_logs")
				.option("properties.bootstrap.servers", "localhost:9092")
				.option("properties.group.id", "test-group")
				.option("scan.startup.mode", "earliest")
				.option("format", "json")
				.option("json.ignore-parse-errors", "true")
				.option("sink.partitioner", "round-robin")
				.schema(
					Schema.newBuilder()
						.column("user_id", DataTypes.BIGINT())
						.column("user_name", DataTypes.STRING())
						.column("score", DataTypes.DECIMAL(10, 2))
						.column("log_ts", DataTypes.STRING())
						.column("part_field_0", DataTypes.STRING())
						.column("part_field_1", DataTypes.INT())
						.column("proc", proctime()) // define a processing-time attribute with column name "proc"
						.column("ts", $("log_ts"))
						.watermark("ts", $("ts").minus(lit(3).seconds()))
						.primaryKey("user_id")
						.build())
				.partitionedBy("part_field_0", "part_field_1") // Kafka doesn't support partitioned table yet, this is just an example for the API
				.build()
		);
	}
}
