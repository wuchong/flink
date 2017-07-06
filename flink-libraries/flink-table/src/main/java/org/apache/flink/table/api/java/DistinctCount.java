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
package org.apache.flink.table.api.java;

import org.apache.flink.table.accumulator.MapAccumulator;
import org.apache.flink.table.functions.AggregateFunction;

public class DistinctCount extends AggregateFunction<Long, MyACC> {

	@Override
	public MyACC createAccumulator() {
		return new MyACC();
	}

	@Override
	public Long getValue(MyACC accumulator) {
		return accumulator.count;
	}

	public void accumulate(MyACC acc, String id) throws Exception {
		if (acc.directory.contains(id)) {
			acc.count++;
		} else {
			acc.directory.put(id, 1);
		}
	}
}

class MyACC {
	MapAccumulator<String, Integer> directory;
	long count = 0;
}
