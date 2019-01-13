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
package org.apache.iotdb.db.query.dataset;

import org.apache.iotdb.db.query.reader.IReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import java.io.IOException;
import java.util.*;

public class EngineDataSetWithoutTimeGenerator extends QueryDataSet {

    private List<IReader> readers;

    private TimeValuePair[] cacheTimeValueList;

    private List<BatchData> batchDataList;

    private List<Boolean> hasDataRemaining;

    private PriorityQueue<Long> timeHeap;

    private Set<Long> timeSet;

    public EngineDataSetWithoutTimeGenerator(List<Path> paths, List<TSDataType> dataTypes, List<IReader> readers)
            throws IOException {
        super(paths, dataTypes);
        this.readers = readers;
        initHeap();
    }

    private void initHeap() throws IOException {
        timeSet = new HashSet<>();
        timeHeap = new PriorityQueue<>();
        cacheTimeValueList = new TimeValuePair[readers.size()];

        for (int i = 0; i < readers.size(); i++) {
            IReader reader = readers.get(i);
            if (reader.hasNext()) {
                TimeValuePair timeValuePair = reader.next();
                cacheTimeValueList[i] = timeValuePair;
                timeHeapPut(timeValuePair.getTimestamp());
            }
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        return timeHeap.size() > 0;
    }

    @Override
    public RowRecord next() throws IOException {
        long minTime = timeHeapGet();

        RowRecord record = new RowRecord(minTime);

        for (int i = 0; i < readers.size(); i++) {
            IReader reader = readers.get(i);
            if (cacheTimeValueList[i] == null) {
                record.addField(new Field(null));
            } else {
                if (cacheTimeValueList[i].getTimestamp() == minTime) {
                    record.addField(getField(cacheTimeValueList[i].getValue(), dataTypes.get(i)));
                    if (readers.get(i).hasNext()) {
                        cacheTimeValueList[i] = reader.next();
                        timeHeapPut(cacheTimeValueList[i].getTimestamp());
                    }
                } else {
                    record.addField(new Field(null));
                }
            }
        }

        return record;
    }

    private Field getField(TsPrimitiveType tsPrimitiveType, TSDataType dataType) {
        Field field = new Field(dataType);
        switch (dataType) {
        case INT32:
            field.setIntV(tsPrimitiveType.getInt());
            break;
        case INT64:
            field.setLongV(tsPrimitiveType.getLong());
            break;
        case FLOAT:
            field.setFloatV(tsPrimitiveType.getFloat());
            break;
        case DOUBLE:
            field.setDoubleV(tsPrimitiveType.getDouble());
            break;
        case BOOLEAN:
            field.setBoolV(tsPrimitiveType.getBoolean());
            break;
        case TEXT:
            field.setBinaryV(tsPrimitiveType.getBinary());
            break;
        default:
            throw new UnSupportedDataTypeException("UnSupported: " + dataType);
        }
        return field;
    }

    /**
     * keep heap from storing duplicate time
     */
    private void timeHeapPut(long time) {
        if (!timeSet.contains(time)) {
            timeSet.add(time);
            timeHeap.add(time);
        }
    }

    private Long timeHeapGet() {
        Long t = timeHeap.poll();
        timeSet.remove(t);
        return t;
    }
}
