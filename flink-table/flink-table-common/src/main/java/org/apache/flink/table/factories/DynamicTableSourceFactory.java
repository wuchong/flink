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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connectors.DynamicTableSource;

/**
 * A factory to create configured table source instances in a batch or stream environment based on
 * string-based properties. See also {@link TableFactory} for more information.
 */
@PublicEvolving
public interface DynamicTableSourceFactory extends TableFactory {

	/**
	 * Creates and configures a {@link DynamicTableSource} based on the given {@link Context}.
	 *
	 * @param context context of this table source.
	 * @return the configured table source.
	 */
	DynamicTableSource createTableSource(Context context);

	/**
	 * Context of table source creation. Contains table information and
	 environment information.
	 */
	interface Context {

		/**
		 * @return full identifier of the given {@link CatalogTable}.
		 */
		ObjectIdentifier getObjectIdentifier();

		/**
		 * @return table {@link CatalogTable} instance.
		 */
		CatalogTable getTable();

		/**
		 * @return readable config of this table environment. The configuration gives the ability
		 * to access {@code TableConfig#getConfiguration()} which holds the current
		 * {@code TableEnvironment} session configurations.
		 */
		ReadableConfig getConfiguration();
	}
}
