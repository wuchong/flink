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

package org.apache.flink.api.java.batch;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.table.BatchTableEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.scala.batch.utils.TableProgramsTestBase;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.TableEnvironment;
import org.apache.flink.api.table.functions.TableFunction;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;


@RunWith(Parameterized.class)
public class UserDefinedTableFunctionITCase extends TableProgramsTestBase {

	public UserDefinedTableFunctionITCase(TestExecutionMode mode, TableConfigMode configMode){
		super(mode, configMode);
	}


	@Test
	public void testUDTF() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds1 =
			CollectionDataSets.getSmall5TupleDataSet(env);

		Table table = tableEnv.fromDataSet(ds1, "a, b, c, d, e");

		tableEnv.registerFunction("stack", new TableFunc0());

		Table result = table.crossApply("stack(a,c) as (f)")
			.select("b,f");

		// with overloading
		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "1,1\n" + "1,0\n" + "2,2\n" + "2,1\n" + "3,2\n" + "3,2\n";
		compareResultAsText(results, expected);

		Table result2 = table.crossApply("stack(a,c,e) as (f)")
			.select("b,f");

		DataSet<Row> ds2 = tableEnv.toDataSet(result2, Row.class);
		List<Row> results2 = ds2.collect();
		String expected2 = "1,1\n" + "1,1\n" + "1,0\n" + "2,2\n" + "2,2\n" + "2,1\n" +
		                   "3,1\n" + "3,2\n" + "3,2\n";
		compareResultAsText(results2, expected2);
	}

	@Test
	public void testUDTFWithOuterApply() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds1 =
			CollectionDataSets.getSmall5TupleDataSet(env);

		Table table = tableEnv.fromDataSet(ds1, "a, b, c, d, e");

		tableEnv.registerFunction("func1", new TableFunc1());

		Table result = table.crossApply("func1(d) as (s,l)")
			.select("d,s,l");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "Hallo Welt,Welt,4\n" + "Hallo Welt wie,Welt,4\n" +
		                  "Hallo Welt wie,wie,3\n";
		compareResultAsText(results, expected);


		Table result2 = table.outerApply("func1(d) as (s,l)")
			.select("d,s,l");

		DataSet<Row> ds2 = tableEnv.toDataSet(result2, Row.class);
		List<Row> results2 = ds2.collect();
		String expected2 = "Hallo,null,null\n" + "Hallo Welt,Welt,4\n" + "Hallo Welt wie,Welt,4\n" +
		                  "Hallo Welt wie,wie,3\n";
		compareResultAsText(results2, expected2);
	}

	@Test
	public void testUDTFWithScalarFunction() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds1 =
			CollectionDataSets.getSmall5TupleDataSet(env);

		Table table = tableEnv.fromDataSet(ds1, "a, b, c, d, e");

		tableEnv.registerFunction("func0", new TableFunc0());

		Table result = table.crossApply("func0(c, charLength(d)) as (l)")
			.select("d,l");

		DataSet<Row> ds = tableEnv.toDataSet(result, Row.class);
		List<Row> results = ds.collect();
		String expected = "Hallo,0\n" + "Hallo,5\n" + "Hallo Welt,1\n" + "Hallo Welt,10\n" +
		                  "Hallo Welt wie,2\n" + "Hallo Welt wie,14\n";
		compareResultAsText(results, expected);
	}


	public static class TableFunc0 extends TableFunction<Integer> {
		public void eval(int a, int b) {
			collect(a);
			collect(b);
		}

		public void eval(int a, int b, long c) {
			collect(a);
			collect(b);
			collect((int) c);
		}
	}

	public static class TableFunc1 extends TableFunction<Tuple2<String, Integer>> {
		public void eval(String str) {
			for (String s : str.split(" ")) {
				if (!s.equals("Hallo")) {
					collect(new Tuple2<>(s, s.length()));
				}
			}
		}
	}

}
