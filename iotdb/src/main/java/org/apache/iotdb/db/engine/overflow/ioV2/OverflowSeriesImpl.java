/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.engine.overflow.ioV2;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.LongStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;

/**
 * This class is only used to store and query overflow overflowIndex {@code IIntervalTreeOperator} data in memory.
 * 
 * @author liukun
 */
public class OverflowSeriesImpl {

    /**
     * The data of update and delete in memory for this time series.
     */
    // public IIntervalTreeOperator overflowIndex;
    private String measurementId;
    private TSDataType dataType;
    private Statistics<Long> statistics;
    private int valueCount;

    public OverflowSeriesImpl(String measurementId, TSDataType dataType) {
        this.measurementId = measurementId;
        this.dataType = dataType;
        statistics = new LongStatistics();
        // overflowIndex = new IntervalTreeOperation(dataType);
    }

    public void insert(long time, byte[] value) {

    }

    public void update(long startTime, long endTime, byte[] value) {
        // overflowIndex.update(startTime, endTime, value);
        statistics.updateStats(startTime, endTime);
        valueCount++;
    }

    public void delete(long timestamp) {
        // overflowIndex.delete(timestamp);
        statistics.updateStats(timestamp, timestamp);
        valueCount++;
    }

    public BatchData query(BatchData data) {
        // return overflowIndex.queryMemory(data);
        return null;
    }

    public long getSize() {
        // return overflowIndex.calcMemSize();
        return 0;
    }

    // public IIntervalTreeOperator getOverflowIndex() {
    // return overflowIndex;
    // }

    public String getMeasurementId() {
        return measurementId;
    }

    public TSDataType getDataType() {
        return dataType;
    }

    public Statistics<Long> getStatistics() {
        return statistics;
    }

    public int getValueCount() {
        return valueCount;
    }
}
