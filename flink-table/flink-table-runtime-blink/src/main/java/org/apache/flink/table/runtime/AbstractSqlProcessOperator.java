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

package org.apache.flink.table.runtime;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.util.Collector;

public abstract class AbstractSqlProcessOperator<K, IN, OUT>
		extends AbstractStreamOperator<OUT>
		implements OneInputStreamOperator<IN, OUT>, Triggerable<K, VoidNamespace> {

	private static final long serialVersionUID = 1L;

	private final long minRetentionTime;
	private final long maxRetentionTime;
	protected final boolean stateCleaningEnabled;

	protected transient Collector<OUT> collector;
	protected transient TimerService timerService;

	// holds the latest registered cleanup timer
	private transient ValueState<Long> cleanupTimeState;

	/** Flag to prevent duplicate function.close() calls in close() and dispose(). */
	private transient boolean operatorClosed = false;

	public AbstractSqlProcessOperator(long minRetentionTime, long maxRetentionTime) {
		this.minRetentionTime = minRetentionTime;
		this.maxRetentionTime = maxRetentionTime;
		this.stateCleaningEnabled = minRetentionTime > 1;
		chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void open() throws Exception {
		super.open();
		this.collector = new TimestampedCollector<>(output);

		InternalTimerService<VoidNamespace> internalTimerService =
			getInternalTimerService("user-timers", VoidNamespaceSerializer.INSTANCE, this);
		this.timerService = new SimpleTimerService(internalTimerService);
	}

	/**
	 * It is notified that no more element will arrive at this operator. This method is
	 * guaranteed to not be called concurrently with other methods of the operator.
	 */
	public void endInput() throws Exception {
	}

	@Override
	public void close() throws Exception {
		super.close();
		endInput();
		operatorClosed = true;
	}

	@Override
	public void dispose() throws Exception {
		if (!operatorClosed) {
			close();
		}
		super.dispose();
	}

	protected void initCleanupTimeState(String stateName) {
		if (stateCleaningEnabled) {
			ValueStateDescriptor<Long> inputCntDescriptor = new ValueStateDescriptor<>(stateName, Types.LONG);
			cleanupTimeState = getRuntimeContext().getState(inputCntDescriptor);
		}
	}

	protected void registerProcessingCleanupTimer(long currentTime) throws Exception {
		if (stateCleaningEnabled) {
			// last registered timer
			Long curCleanupTime = cleanupTimeState.value();

			// check if a cleanup timer is registered and
			// that the current cleanup timer won't delete state we need to keep
			if (curCleanupTime == null || (currentTime + minRetentionTime) > curCleanupTime) {
				// we need to register a new (later) timer
				long cleanupTime = currentTime + maxRetentionTime;
				// register timer and remember clean-up time
				timerService.registerProcessingTimeTimer(cleanupTime);
				// delete expired timer
				if (curCleanupTime != null) {
					timerService.deleteProcessingTimeTimer(curCleanupTime);
				}
				cleanupTimeState.update(cleanupTime);
			}
		}
	}

	protected void cleanupState(State... states) {
		for (State state : states) {
			state.clear();
		}
		this.cleanupTimeState.clear();
	}

}
