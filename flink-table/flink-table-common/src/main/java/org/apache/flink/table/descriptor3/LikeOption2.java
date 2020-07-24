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

public enum  LikeOption2 {
	INCLUDING_ALL,
	INCLUDING_CONSTRAINTS,
	INCLUDING_GENERATED,
	INCLUDING_OPTIONS,
	INCLUDING_PARTITIONS,
	INCLUDING_WATERMARKS,

	EXCLUDING_ALL,
	EXCLUDING_CONSTRAINTS,
	EXCLUDING_GENERATED,
	EXCLUDING_OPTIONS,
	EXCLUDING_PARTITIONS,
	EXCLUDING_WATERMARKS,

	OVERWRITING_CONSTRAINTS,
	OVERWRITING_GENERATED,
	OVERWRITING_OPTIONS,
	OVERWRITING_PARTITIONS,
	OVERWRITING_WATERMARKS
}
