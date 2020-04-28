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

package org.apache.flink.api.java.io.jdbc;

import org.apache.flink.connectors.jdbc.JdbcReadOptions;


/**
 * Options for the JDBC scan.
 *
 * @deprecated Please use {@link JdbcReadOptions}, Flink proposes class name start with "Jdbc" rather than "JDBC".
 */
@Deprecated
public class JDBCReadOptions extends JdbcReadOptions {

	private JDBCReadOptions(
			String partitionColumnName,
			Long partitionLowerBound,
			Long partitionUpperBound,
			Integer numPartitions,
			int fetchSize) {
		super(partitionColumnName, partitionLowerBound, partitionUpperBound, numPartitions, fetchSize);
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder for {@link JdbcReadOptions}.
	 */
	public static class Builder extends JdbcReadOptions.Builder {
		/**
		 * optional, name of the column used for partitioning the input.
		 */
		public Builder setPartitionColumnName(String partitionColumnName) {
			this.partitionColumnName = partitionColumnName;
			return this;
		}

		/**
		 * optional, the smallest value of the first partition.
		 */
		public Builder setPartitionLowerBound(long partitionLowerBound) {
			this.partitionLowerBound = partitionLowerBound;
			return this;
		}

		/**
		 * optional, the largest value of the last partition.
		 */
		public Builder setPartitionUpperBound(long partitionUpperBound) {
			this.partitionUpperBound = partitionUpperBound;
			return this;
		}

		/**
		 * optional, the maximum number of partitions that can be used for parallelism in table reading.
		 */
		public Builder setNumPartitions(int numPartitions) {
			this.numPartitions = numPartitions;
			return this;
		}

		/**
		 * optional, the number of rows to fetch per round trip.
		 * default value is 0, according to the jdbc api, 0 means that fetchSize hint will be ignored.
		 */
		public Builder setFetchSize(int fetchSize) {
			this.fetchSize = fetchSize;
			return this;
		}

		public JDBCReadOptions build() {
			return new JDBCReadOptions(
				partitionColumnName, partitionLowerBound, partitionUpperBound, numPartitions, fetchSize);
		}
	}
}
