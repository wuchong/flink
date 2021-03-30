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

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.TestUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.types.RowKind.DELETE;
import static org.apache.flink.types.RowKind.INSERT;
import static org.apache.flink.types.RowKind.UPDATE_AFTER;
import static org.junit.Assert.assertEquals;

/** Test for {@link BufferedUpsertKafkaSinkFunction}. */
public class BufferedUpsertKafkaSinkFunctionTest {

    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(
                    Column.physical("id", DataTypes.INT().notNull()),
                    Column.physical("title", DataTypes.STRING().notNull()),
                    Column.physical("author", DataTypes.STRING()),
                    Column.physical("price", DataTypes.DOUBLE()),
                    Column.physical("qty", DataTypes.INT()),
                    Column.physical("ts", DataTypes.TIMESTAMP(3)));

    private static final int keyIndices = 0;
    private static final int TIMESTAMP_INDICES = 5;
    private static final int BATCH_SIZE = 4;
    private static final Duration FLUSH_INTERVAL = Duration.ofMillis(60_000);

    public static final RowData[] TEST_DATA = {
        GenericRowData.ofKind(
                INSERT,
                1001,
                StringData.fromString("Java public for dummies"),
                StringData.fromString("Tan Ah Teck"),
                11.11,
                11,
                TimestampData.fromInstant(Instant.parse("2021-03-30T15:00:00Z"))),
        GenericRowData.ofKind(
                INSERT,
                1002,
                StringData.fromString("More Java for dummies"),
                StringData.fromString("Tan Ah Teck"),
                22.22,
                22,
                TimestampData.fromInstant(Instant.parse("2021-03-30T16:00:00Z"))),
        GenericRowData.ofKind(
                INSERT,
                1004,
                StringData.fromString("A Cup of Java"),
                StringData.fromString("Kumar"),
                44.44,
                44,
                TimestampData.fromInstant(Instant.parse("2021-03-30T17:00:00Z"))),
        GenericRowData.ofKind(
                UPDATE_AFTER,
                1004,
                StringData.fromString("A Teaspoon of Java"),
                StringData.fromString("Kevin Jones"),
                55.55,
                55,
                TimestampData.fromInstant(Instant.parse("2021-03-30T18:00:00Z"))),
        GenericRowData.ofKind(
                UPDATE_AFTER,
                1004,
                StringData.fromString("A Teaspoon of Java 1.4"),
                StringData.fromString("Kevin Jones"),
                66.66,
                66,
                TimestampData.fromInstant(Instant.parse("2021-03-30T19:00:00Z"))),
        GenericRowData.ofKind(
                UPDATE_AFTER,
                1004,
                StringData.fromString("A Teaspoon of Java 1.5"),
                StringData.fromString("Kevin Jones"),
                77.77,
                77,
                TimestampData.fromInstant(Instant.parse("2021-03-30T20:00:00Z"))),
        GenericRowData.ofKind(
                DELETE,
                1004,
                StringData.fromString("A Teaspoon of Java 1.8"),
                StringData.fromString("Kevin Jones"),
                null,
                1010,
                TimestampData.fromInstant(Instant.parse("2021-03-30T21:00:00Z")))
    };

    private static KafkaDynamicSink.SinkFunctionProviderCreator creator =
            BufferedUpsertKafkaSinkFunction.createBufferedSinkFunction(
                    SCHEMA.toSinkRowDataType(), new int[] {keyIndices}, BATCH_SIZE, FLUSH_INTERVAL);

    @Test
    public void testWirteData() throws Exception {
        MockedSinkFunction sinkFunction = new MockedSinkFunction();
        createBufferedSinkAndWriteData(sinkFunction);

        HashMap<Integer, List<RowData>> expected = new HashMap<>();
        expected.put(
                1001,
                Collections.singletonList(
                        GenericRowData.ofKind(
                                UPDATE_AFTER,
                                1001,
                                StringData.fromString("Java public for dummies"),
                                StringData.fromString("Tan Ah Teck"),
                                11.11,
                                11,
                                TimestampData.fromInstant(Instant.parse("2021-03-30T15:00:00Z")))));
        expected.put(
                1002,
                Collections.singletonList(
                        GenericRowData.ofKind(
                                UPDATE_AFTER,
                                1002,
                                StringData.fromString("More Java for dummies"),
                                StringData.fromString("Tan Ah Teck"),
                                22.22,
                                22,
                                TimestampData.fromInstant(Instant.parse("2021-03-30T16:00:00Z")))));
        expected.put(
                1004,
                Collections.singletonList(
                        GenericRowData.ofKind(
                                UPDATE_AFTER,
                                1004,
                                StringData.fromString("A Teaspoon of Java"),
                                StringData.fromString("Kevin Jones"),
                                55.55,
                                55,
                                TimestampData.fromInstant(Instant.parse("2021-03-30T18:00:00Z")))));

        compareCompactedResult(expected, sinkFunction.rowDataCollectors);
    }

    @Test
    public void testFlushDataWhenCheckpointing() throws Exception {
        MockedSinkFunction sinkFunction = new MockedSinkFunction();
        BufferedUpsertKafkaSinkFunction bufferedFunction =
                createBufferedSinkAndWriteData(sinkFunction);

        bufferedFunction.snapshotState(null);

        HashMap<Integer, List<RowData>> expected = new HashMap<>();
        expected.put(
                1001,
                Collections.singletonList(
                        GenericRowData.ofKind(
                                UPDATE_AFTER,
                                1001,
                                StringData.fromString("Java public for dummies"),
                                StringData.fromString("Tan Ah Teck"),
                                11.11,
                                11,
                                TimestampData.fromInstant(Instant.parse("2021-03-30T15:00:00Z")))));
        expected.put(
                1002,
                Collections.singletonList(
                        GenericRowData.ofKind(
                                UPDATE_AFTER,
                                1002,
                                StringData.fromString("More Java for dummies"),
                                StringData.fromString("Tan Ah Teck"),
                                22.22,
                                22,
                                TimestampData.fromInstant(Instant.parse("2021-03-30T16:00:00Z")))));
        expected.put(
                1004,
                Arrays.asList(
                        GenericRowData.ofKind(
                                UPDATE_AFTER,
                                1004,
                                StringData.fromString("A Teaspoon of Java"),
                                StringData.fromString("Kevin Jones"),
                                55.55,
                                55,
                                TimestampData.fromInstant(Instant.parse("2021-03-30T18:00:00Z"))),
                        GenericRowData.ofKind(
                                DELETE,
                                1004,
                                StringData.fromString("A Teaspoon of Java 1.8"),
                                StringData.fromString("Kevin Jones"),
                                null,
                                1010,
                                TimestampData.fromInstant(Instant.parse("2021-03-30T21:00:00Z")))));

        compareCompactedResult(expected, sinkFunction.rowDataCollectors);
    }

    // --------------------------------------------------------------------------------------------

    private BufferedUpsertKafkaSinkFunction createBufferedSinkAndWriteData(
            MockedSinkFunction sinkFunction) throws Exception {
        BufferedUpsertKafkaSinkFunction bufferedSinkFunction =
                (BufferedUpsertKafkaSinkFunction)
                        creator.create(sinkFunction, 1).createSinkFunction();

        bufferedSinkFunction.open(new Configuration());

        for (RowData row : TEST_DATA) {
            bufferedSinkFunction.invoke(
                    row,
                    new TestUtils.MockSinkContext(
                            row.getTimestamp(TIMESTAMP_INDICES, 3).getMillisecond(),
                            Long.MIN_VALUE,
                            Long.MIN_VALUE));
        }
        return bufferedSinkFunction;
    }

    private void compareCompactedResult(
            Map<Integer, List<RowData>> expected, List<RowData> actual) {
        Map<Integer, List<RowData>> actualMap = new HashMap<>();

        for (RowData rowData : actual) {
            Integer id = rowData.getInt(keyIndices);
            actualMap.computeIfAbsent(id, key -> new ArrayList<>()).add(rowData);
        }

        assertEquals(expected.size(), actualMap.size());
        for (Integer id : expected.keySet()) {
            assertEquals(expected.get(id), actualMap.get(id));
        }
    }

    // --------------------------------------------------------------------------------------------

    static class MockedSinkFunction extends RichSinkFunction<RowData>
            implements CheckpointedFunction, CheckpointListener {

        transient List<RowData> rowDataCollectors;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            rowDataCollectors = new ArrayList<>();
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            // do nothing
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // do nothing
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // do nothing
        }

        @Override
        public void invoke(RowData value, Context context) throws Exception {
            assertEquals(
                    value.getTimestamp(5, 3).toInstant(),
                    Instant.ofEpochMilli(context.timestamp()));
            rowDataCollectors.add(value);
        }
    }
}
