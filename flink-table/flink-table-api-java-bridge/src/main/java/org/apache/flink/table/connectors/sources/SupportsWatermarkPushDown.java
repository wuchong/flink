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

package org.apache.flink.table.connectors.sources;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.table.dataformats.RowData;

import javax.annotation.Nullable;

import java.util.Optional;

/**
 * Allows to push down watermarks into a {@link DynamicTableSource ) if it {@link SupportsChangelogReading }.
 */
@PublicEvolving
public interface SupportsWatermarkPushDown extends SupportsChangelogReading {

	void applyWatermark(WatermarkAssigner assigner);

	// --------------------------------------------------------------------------------------------

	final class WatermarkAssigner {

		private @Nullable AssignerWithPeriodicWatermarks<RowData> periodicAssigner;

		private @Nullable AssignerWithPunctuatedWatermarks<RowData> punctuatedAssigner;

		private WatermarkAssigner(
				@Nullable AssignerWithPeriodicWatermarks<RowData> periodicAssigner,
				@Nullable AssignerWithPunctuatedWatermarks<RowData> punctuatedAssigner) {
			this.periodicAssigner = periodicAssigner;
			this.punctuatedAssigner = punctuatedAssigner;
		}

		public static WatermarkAssigner periodic(AssignerWithPeriodicWatermarks<RowData> periodicWatermarks) {
			return new WatermarkAssigner(periodicWatermarks, null);
		}

		public static WatermarkAssigner punctuated(AssignerWithPunctuatedWatermarks<RowData> punctuatedWatermarks) {
			return new WatermarkAssigner(null, punctuatedWatermarks);
		}

		public static WatermarkAssigner undefined() {
			return new WatermarkAssigner(null, null);
		}

		public boolean isUndefined() {
			return periodicAssigner == null && punctuatedAssigner == null;
		}

		public Optional<AssignerWithPeriodicWatermarks<RowData>> getPeriodicAssigner() {
			return Optional.ofNullable(periodicAssigner);
		}

		public Optional<AssignerWithPunctuatedWatermarks<RowData>> getPunctuatedAssigner() {
			return Optional.ofNullable(punctuatedAssigner);
		}
	}
}
