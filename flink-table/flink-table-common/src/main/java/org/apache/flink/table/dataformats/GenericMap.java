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

package org.apache.flink.table.dataformats;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Map;
import java.util.Objects;

/**
 * A GenericMap is a map where all the keys have the same type, and all the values have the same type.
 * It can be considered as a wrapper class of the normal java map.
 */
@PublicEvolving
public class GenericMap implements BaseMap {

	private static final long serialVersionUID = 1L;

	private final Map<?, ?> map;

	public GenericMap(Map<?, ?> map) {
		this.map = map;
	}

	@Override
	public int numElements() {
		return map.size();
	}

	@Override
	public Map<?, ?> toJavaMap(LogicalType keyType, LogicalType valueType) {
		return map;
	}

	public Map<?, ?> getJavaMap() {
		return map;
	}

	@Override
	public BaseArray keyArray() {
		return null;
	}

	@Override
	public BaseArray valueArray() {
		return null;
	}

	public Object get(Object key) {
		return map.get(key);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		GenericMap that = (GenericMap) o;
		return Objects.equals(map, that.map);
	}

	@Override
	public int hashCode() {
		return Objects.hash(map);
	}

	@Override
	public String toString() {
		return map.toString();
	}
}
