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

package org.apache.flink.table.descriptor3;

import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

public class Connector extends TableDescriptor {

	public static ConnectorBuilder of(String identifier) {
		return new ConnectorBuilder(identifier);
	}

	public static class ConnectorBuilder extends TableDescriptorBuilder {

		private ConnectorBuilder(String identifier) {
			super(Connector.class);
			option(CONNECTOR.key(), identifier);
		}

		public ConnectorBuilder option(String key, String value) {
			String lowerKey = key.toLowerCase().trim();
			if (CONNECTOR.key().equals(lowerKey)) {
				throw new IllegalArgumentException("It's not allowed to override 'connector' option.");
			}
			return (ConnectorBuilder) super.option(key, value);
		}

		@Override
		public ConnectorBuilder schema(Schema schema) {
			return (ConnectorBuilder) super.schema(schema);
		}

		@Override
		public ConnectorBuilder partitionedBy(String... fieldNames) {
			return (ConnectorBuilder) super.partitionedBy(fieldNames);
		}

		@Override
		public ConnectorBuilder like(String tablePath, LikeOption... likeOptions) {
			return (ConnectorBuilder) super.like(tablePath, likeOptions);
		}

		@Override
		public Connector build() {
			return (Connector) super.build();
		}
	}

}
