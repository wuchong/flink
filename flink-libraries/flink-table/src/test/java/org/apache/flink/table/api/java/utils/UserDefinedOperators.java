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
package org.apache.flink.table.api.java.utils;

import com.google.common.collect.MinMaxPriorityQueue;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.functions.MultisetAggregateFunction;
import org.apache.flink.table.functions.RichTableFunction;
import org.apache.flink.types.Row;

import java.util.*;

public class UserDefinedOperators {

	public static class JoinAcc {
		Map<Integer, String> left = new HashMap<>();
		Map<Integer, Double> right = new HashMap<>();
	}

	public static class JoinOperator extends RichTableFunction<Row, JoinAcc> {

		private final Row reuse = new Row(3);

		@Override
		public JoinAcc createAccumulator() {
			return new JoinAcc();
		}

		public void eval(JoinAcc acc, Integer id1, String itemName, Integer id2, Double price, boolean leftOrRight) {
			if (leftOrRight) {
				acc.left.put(id1, itemName);
				Double rightPrice = acc.right.get(id1);
				if (rightPrice != null) {
					emit(id1, itemName, rightPrice);
				}
			} else {
				acc.right.put(id2, price);
				String leftItemName = acc.left.get(id2);
				if (leftItemName != null) {
					emit(id2, leftItemName, price);
				}
			}
		}

		private void emit(Integer id, String itemName, Double price) {
			reuse.setField(0, id);
			reuse.setField(1, itemName);
			reuse.setField(2, price);
			collect(reuse);
		}

		@Override
		public TypeInformation<Row> getResultType() {
			TypeInformation<?>[] types = new TypeInformation[]{Types.INT(), Types.STRING(), Types.DOUBLE()};
			String[] fields = {"id", "name", "price"};
			return new RowTypeInfo(types, fields);
		}
	}

	public static class CountAcc {
		public Long count = 0L;
	}

	public static class EvenFilterOperator extends RichTableFunction<Row, CountAcc> {

		@Override
		public CountAcc createAccumulator() {
			return new CountAcc();
		}

		private final Row reuse = new Row(2);

		public void eval(CountAcc acc, int id, String name) {
			acc.count += 1;
			if (acc.count % 2 == 0) {
				reuse.setField(0, id);
				reuse.setField(1, name);
				collect(reuse);
			}
		}

		@Override
		public TypeInformation<Row> getResultType() {
			TypeInformation<?>[] types = new TypeInformation[]{Types.INT(), Types.STRING()};
			String[] fields = {"id", "name"};
			return new RowTypeInfo(types, fields);
		}
	}

	public static class TopN extends MultisetAggregateFunction<Tuple2<String, Integer>, Map<String, Integer>> {

		private final int n;

		public TopN(int n) {
			this.n = n;
		}


		@Override
		public Map<String, Integer> createAccumulator() {
			return new HashMap<>();
		}

		public void accumulate(Map<String, Integer> acc, String key, int sum) {
			if (acc.containsKey(key)) {
				acc.put(key, acc.get(key) + sum);
			} else {
				acc.put(key, sum);
			}
		}

		@Override
		public List<Tuple2<String, Integer>> getTable(Map<String, Integer> accumulator) {
			MinMaxPriorityQueue<Tuple2<String, Integer>> queue = MinMaxPriorityQueue
				.orderedBy(new MyComparator())
				.maximumSize(n)
				.create();

			for (Map.Entry<String, Integer> entry : accumulator.entrySet()) {
				queue.add(Tuple2.of(entry.getKey(), entry.getValue()));
			}
			List<Tuple2<String, Integer>> result = new ArrayList<>();
			Tuple2<String, Integer> tuple = queue.pollFirst();
			while (tuple != null) {
				result.add(tuple);
				tuple = queue.pollFirst();
			}
			return result;
		}
	}


	static class MyComparator implements Comparator<Tuple2<String, Integer>> {

		@Override
		public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
			return o2.f1 - o1.f1;
		}

	}

}
