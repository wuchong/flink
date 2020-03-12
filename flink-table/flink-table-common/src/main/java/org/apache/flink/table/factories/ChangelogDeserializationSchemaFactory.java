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

package org.apache.flink.table.factories;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connectors.ChangelogDeserializationSchema;
import org.apache.flink.table.dataformats.SqlRow;

import java.util.Map;

/**
 * Factory for creating configured instances of {@link org.apache.flink.table.connectors.ChangelogDeserializationSchema}.
 */
@PublicEvolving
public interface ChangelogDeserializationSchemaFactory extends TableFormatFactory<SqlRow> {

	/**
	 * Creates and configures a {@link ChangelogDeserializationSchema} using the given properties.
	 *
	 * @param properties normalized properties describing the format
	 * @return the configured deserialization schema or null if the factory cannot provide an
	 *         instance of this class
	 */
	ChangelogDeserializationSchema createDeserializationSchema(Map<String, String> properties);

}
