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

import org.apache.flink.table.descriptor2.Connector;
import org.apache.flink.table.descriptor2.Json;
import org.apache.flink.table.descriptor2.Kafka;
import org.apache.flink.table.descriptor2.LikeOption;
import org.apache.flink.table.descriptor2.Schema;

import org.junit.Test;

import java.time.Duration;

public class TableDescriptor2Test {

	TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());

	public void testV4() {
Schema schema = new Schema()
	.column("user_id", DataTypes.BIGINT())
	.column("score", DataTypes.DECIMAL(10, 2))
	.column("ts", DataTypes.TIMESTAMP(3));
Table myKafka = tEnv.from(
	new Kafka()
		.version("0.11")
		.topic("user_logs")
		.property("bootstrap.servers", "localhost:9092")
		.property("group.id", "test-group")
		.startFromEarliest()
		.sinkPartitionerRoundRobin()
		.format(new Json().ignoreParseErrors(false))
		.schema(schema)
);
// reading from kafka table and write into filesystem table
myKafka.executeInsert(
	new Connector("filesystem")
		.option("path", "/path/to/whatever")
		.option("format", "json")
	.schema(schema)
);
	}

	@Test
	public void testLike() {
tEnv.createTemporaryTable(
	"OrdersInKafka",
	new Kafka()
		.topic("user_logs")
		.property("bootstrap.servers", "localhost:9092")
		.property("group.id", "test-group")
		.format(new Json().ignoreParseErrors(false))
		.schema(
			new Schema()
				.column("user_id", DataTypes.BIGINT())
				.column("score", DataTypes.DECIMAL(10, 2))
				.column("log_ts", DataTypes.TIMESTAMP(3))
				.computedColumn("my_ts", "TO_TIMESTAMP(log_ts)")
		)
);

tEnv.createTemporaryTable(
	"OrdersInFilesystem",
	new Connector("filesystem")
		.option("path", "path/to/whatever")
		.schema(
			new Schema()
				.watermarkFor("ts").boundedOutOfOrderTimestamps(Duration.ofSeconds(5)))
		.like("OrdersInKafka", LikeOption.EXCLUDING.ALL, LikeOption.INCLUDING.GENERATED)
);

tEnv.createTemporaryTable(
	"OrdersInFilesystem",
	new Connector("filesystem")
		.option("path", "path/to/whatever")
		.schema(
			new Schema()
				.column("user_id", DataTypes.BIGINT())
				.column("score", DataTypes.DECIMAL(10, 2))
				.column("log_ts", DataTypes.TIMESTAMP(3))
				.computedColumn("my_ts", "TO_TIMESTAMP(log_ts)")
				.watermarkFor("ts").boundedOutOfOrderTimestamps(Duration.ofSeconds(5))
		)
);
	}

	@Test
	public void testV3() {
// register a table using specific descriptor
tEnv.createTemporaryTable(
	"MyTable",
	new Kafka()
		.version("0.11")
		.topic("user_logs")
		.property("bootstrap.servers", "localhost:9092")
		.property("group.id", "test-group")
		.startFromEarliest()
		.sinkPartitionerRoundRobin()
		.format(new Json().ignoreParseErrors(false))
		.schema(
			new Schema()
				.column("user_id", DataTypes.BIGINT())
				.column("user_name", DataTypes.STRING())
				.column("score", DataTypes.DECIMAL(10, 2))
				.column("log_ts", DataTypes.STRING())
				.column("part_field_0", DataTypes.STRING())
				.column("part_field_1", DataTypes.INT())
				.proctime("proc") // define a processing-time attribute with column name "proc"
				.computedColumn("my_ts", "TO_TIMESTAMP(log_ts)")  // computed column
				.watermarkFor("my_ts").boundedOutOfOrderTimestamps(Duration.ofSeconds(5))  // defines watermark and rowtime attribute
				.primaryKey("user_id"))
		.partitionedBy("part_field_0", "part_field_1")  // Kafka doesn't support partitioned table yet, this is just an example for the API
);

// register a table using general purpose Connector descriptor, this would be helpful for custom source/sinks
tEnv.createTemporaryTable(
	"MyTable",
	new Connector("kafka-0.11")
		.option("topic", "user_logs")
		.option("properties.bootstrap.servers", "localhost:9092")
		.option("properties.group.id", "test-group")
		.option("scan.startup.mode", "earliest")
		.option("format", "json")
		.option("json.ignore-parse-errors", "true")
		.option("sink.partitioner", "round-robin")
		.schema(
			new Schema()
				.column("user_id", DataTypes.BIGINT())
				.column("user_name", DataTypes.STRING())
				.column("score", DataTypes.DECIMAL(10, 2))
				.column("log_ts", DataTypes.STRING())
				.column("part_field_0", DataTypes.STRING())
				.column("part_field_1", DataTypes.INT())
				.proctime("proc") // define a processing-time attribute with column name "proc"
				.computedColumn("my_ts", "TO_TIMESTAMP(log_ts)")  // computed column
				.watermarkFor("my_ts").boundedOutOfOrderTimestamps(Duration.ofSeconds(5)) // defines watermark and rowtime attribute
				.primaryKey("user_id"))
		.partitionedBy("part_field_0", "part_field_1") // Kafka doesn't support partitioned table yet, this is just an example for the API
);
	}
}
