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
package org.apache.iotdb.db.engine.querycontext;

import org.apache.iotdb.db.engine.memtable.TimeValuePairSorter;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.db.engine.memtable.TimeValuePairSorter;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TsPrimitiveType;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

//TODO: merge ReadOnlyMemChunk and WritableMemChunk and IWritableMemChunk
public class ReadOnlyMemChunk implements TimeValuePairSorter {

    private boolean initialized;

    private TSDataType dataType;
    private TimeValuePairSorter memSeries;
    private List<TimeValuePair> sortedTimeValuePairList;

    public ReadOnlyMemChunk(TSDataType dataType, TimeValuePairSorter memSeries) {
        this.dataType = dataType;
        this.memSeries = memSeries;
        this.initialized = false;
    }

    private void checkInitialized() {
        if (!initialized) {
            init();
        }
    }

    private void init() {
        sortedTimeValuePairList = memSeries.getSortedTimeValuePairList();
        initialized = true;
    }

    /**
     * only for test now.
     * 
     * @return
     */
    public TSDataType getDataType() {
        return dataType;
    }

    /**
     * only for test now.
     * 
     * @return
     */
    public long getMaxTimestamp() {
        checkInitialized();
        if (!isEmpty()) {
            return sortedTimeValuePairList.get(sortedTimeValuePairList.size() - 1).getTimestamp();
        } else {
            return -1;
        }
    }

    /**
     * only for test now.
     * 
     * @return
     */
    public long getMinTimestamp() {
        checkInitialized();
        if (!isEmpty()) {
            return sortedTimeValuePairList.get(0).getTimestamp();
        } else {
            return -1;
        }
    }

    /**
     * only for test now.
     * 
     * @return
     */
    public TsPrimitiveType getValueAtMaxTime() {
        checkInitialized();
        if (!isEmpty()) {
            return sortedTimeValuePairList.get(sortedTimeValuePairList.size() - 1).getValue();
        } else {
            return null;
        }
    }

    /**
     * only for test now.
     * 
     * @return
     */
    public TsPrimitiveType getValueAtMinTime() {
        checkInitialized();
        if (!isEmpty()) {
            return sortedTimeValuePairList.get(0).getValue();
        } else {
            return null;
        }
    }

    @Override
    public List<TimeValuePair> getSortedTimeValuePairList() {
        checkInitialized();
        return Collections.unmodifiableList(sortedTimeValuePairList);
    }

    @Override
    public Iterator<TimeValuePair> getIterator() {
        checkInitialized();
        return sortedTimeValuePairList.iterator();
    }

    @Override
    public boolean isEmpty() {
        checkInitialized();
        return sortedTimeValuePairList.size() == 0;
    }
}
