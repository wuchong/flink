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

package org.apache.flink.table.runtime.aggregate;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.JoinedRow;
import org.apache.flink.table.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.generated.AggsHandleFunction;
import org.apache.flink.table.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.generated.RecordEqualiser;
import org.apache.flink.table.runtime.AbstractSqlProcessOperator;
import org.apache.flink.table.type.InternalType;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

import static org.apache.flink.table.dataformat.util.BaseRowUtil.ACCUMULATE_MSG;
import static org.apache.flink.table.dataformat.util.BaseRowUtil.RETRACT_MSG;
import static org.apache.flink.table.dataformat.util.BaseRowUtil.isAccumulateMsg;
import static org.apache.flink.table.dataformat.util.BinaryRowUtil.EMPTY_ROW;

/**
 * Aggregate Operator used for the groupby (without window) aggregate.
 */
public class GroupAggOperator extends AbstractSqlProcessOperator<BaseRow, BaseRow, BaseRow> {

	private static final long serialVersionUID = -5114710574770220334L;

	/**
	 * The code generated function used to handle aggregates.
	 */
	private final GeneratedAggsHandleFunction genAggsHandler;

	/**
	 * The code generated equaliser used to equal BaseRow.
	 */
	private final GeneratedRecordEqualiser genRecordEqualiser;

	/**
	 * The accumulator types.
	 */
	private final InternalType[] accTypes;

	/**
	 * Used to count the number of added and retracted input records.
	 */
	private final RecordCounter recordCounter;

	/**
	 * Whether this operator will generate retraction.
	 */
	private final boolean generateRetraction;

	/**
	 * Whether group keys of this aggregate is emtpy.
	 */
	private final boolean groupWithoutKey;

	/**
	 * Reused output row.
	 */
	private transient JoinedRow resultRow = null;

	// function used to handle all aggregates
	private transient AggsHandleFunction function = null;

	// function used to equal BaseRow
	private transient RecordEqualiser equaliser = null;

	// stores the accumulators
	private transient ValueState<BaseRow> accState = null;

	/**
	 * Creates a {@link GroupAggFunction}.
	 *  @param minRetentionTime minimal state idle retention time.
	 * @param maxRetentionTime maximal state idle retention time.
	 * @param genAggsHandler The code generated function used to handle aggregates.
	 * @param genRecordEqualiser The code generated equaliser used to equal BaseRow.
	 * @param accTypes The accumulator types.
	 * @param indexOfCountStar The index of COUNT(*) in the aggregates.
	 *                          -1 when the input doesn't contain COUNT(*), i.e. doesn't contain retraction messages.
	 *                          We make sure there is a COUNT(*) if input stream contains retraction.
	 * @param generateRetraction Whether this operator will generate retraction.
	 * @param groupWithoutKey Whether group keys of this aggregate is emtpy.
	 */
	public GroupAggOperator(
			long minRetentionTime,
			long maxRetentionTime,
			GeneratedAggsHandleFunction genAggsHandler,
			GeneratedRecordEqualiser genRecordEqualiser,
			InternalType[] accTypes,
			int indexOfCountStar,
			boolean generateRetraction,
			boolean groupWithoutKey) {
		super(minRetentionTime, maxRetentionTime);
		this.genAggsHandler = genAggsHandler;
		this.genRecordEqualiser = genRecordEqualiser;
		this.accTypes = accTypes;
		this.recordCounter = RecordCounter.of(indexOfCountStar);
		this.generateRetraction = generateRetraction;
		this.groupWithoutKey = groupWithoutKey;
	}

	@Override
	public void open() throws Exception {
		super.open();
		// instantiate function
		function = genAggsHandler.newInstance(getRuntimeContext().getUserCodeClassLoader());
		function.open(new PerKeyStateDataViewStore(getRuntimeContext()));
		// instantiate equaliser
		equaliser = genRecordEqualiser.newInstance(getRuntimeContext().getUserCodeClassLoader());

		BaseRowTypeInfo accTypeInfo = new BaseRowTypeInfo(accTypes);
		ValueStateDescriptor<BaseRow> accDesc = new ValueStateDescriptor<>("accState", accTypeInfo);
		accState = getRuntimeContext().getState(accDesc);

		initCleanupTimeState("GroupAggregateCleanupTime");

		resultRow = new JoinedRow();
	}

	@Override
	public void processElement(StreamRecord<BaseRow> element) throws Exception {
		BaseRow input = element.getValue();
		long currentTime = timerService.currentProcessingTime();
		// register state-cleanup timer
		registerProcessingCleanupTimer(currentTime);

		BaseRow currentKey = (BaseRow) getCurrentKey();

		boolean firstRow;
		BaseRow accumulators = accState.value();
		if (null == accumulators) {
			firstRow = true;
			accumulators = function.createAccumulators();
		} else {
			firstRow = false;
		}

		// set accumulators to handler first
		function.setAccumulators(accumulators);
		// get previous aggregate result
		BaseRow prevAggValue = function.getValue();

		// update aggregate result and set to the newRow
		if (isAccumulateMsg(input)) {
			// accumulate input
			function.accumulate(input);
		} else {
			// retract input
			function.retract(input);
		}
		// get current aggregate result
		BaseRow newAggValue = function.getValue();

		// get accumulator
		accumulators = function.getAccumulators();

		if (!recordCounter.recordCountIsZero(accumulators)) {
			// we aggregated at least one record for this key

			// update the state
			accState.update(accumulators);

			// if this was not the first row and we have to emit retractions
			if (!firstRow) {
				if (!stateCleaningEnabled && equaliser.equalsWithoutHeader(prevAggValue, newAggValue)) {
					// newRow is the same as before and state cleaning is not enabled.
					// We do not emit retraction and acc message.
					// If state cleaning is enabled, we have to emit messages to prevent too early
					// state eviction of downstream operators.
					return;
				} else {
					// retract previous result
					if (generateRetraction) {
						// prepare retraction message for previous row
						resultRow.replace(currentKey, prevAggValue).setHeader(RETRACT_MSG);
						collector.collect(resultRow);
					}
				}
			}
			// emit the new result
			resultRow.replace(currentKey, newAggValue).setHeader(ACCUMULATE_MSG);
			collector.collect(resultRow);

		} else {
			// we retracted the last record for this key
			// sent out a delete message
			if (!firstRow) {
				// prepare delete message for previous row
				resultRow.replace(currentKey, prevAggValue).setHeader(RETRACT_MSG);
				collector.collect(resultRow);
			}
			// and clear all state
			accState.clear();
			// cleanup dataview under current key
			function.cleanup();
		}
	}

	@Override
	public void onEventTime(InternalTimer<BaseRow, VoidNamespace> timer) throws Exception {
		// nothing to do
	}

	@Override
	public void onProcessingTime(InternalTimer<BaseRow, VoidNamespace> timer) throws Exception {
		if (stateCleaningEnabled) {
			cleanupState(accState);
			function.cleanup();
		}
	}

	@Override
	public void endInput() throws Exception {
		// output default value if grouping without key and it's an empty group,
		// this is only used in test when input is bounded.
		if (groupWithoutKey) {
			setCurrentKey(EMPTY_ROW);
			BaseRow acc = accState.value();
			if (recordCounter.recordCountIsZero(acc)) {
				function.setAccumulators(function.createAccumulators());
				BaseRow newAggValue = function.getValue();
				resultRow.replace(EMPTY_ROW, newAggValue).setHeader(ACCUMULATE_MSG);
			}
		}
		// no need to update acc state, because this is the end
	}

	@Override
	public void close() throws Exception {
		super.close();
		if (function != null) {
			function.close();
		}
	}
}
