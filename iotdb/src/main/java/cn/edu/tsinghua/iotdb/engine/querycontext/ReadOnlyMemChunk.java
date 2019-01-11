package cn.edu.tsinghua.iotdb.engine.querycontext;

import cn.edu.tsinghua.iotdb.engine.memtable.TimeValuePairSorter;
import cn.edu.tsinghua.iotdb.utils.TimeValuePair;
import cn.edu.tsinghua.iotdb.utils.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

//TODO: merge ReadOnlyMemChunk and WritableMemChunk and IWritableMemChunk
public class ReadOnlyMemChunk implements TimeValuePairSorter{

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
     * @return
     */
    public TSDataType getDataType() {
        return dataType;
    }

    /**
     * only for test now.
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
    public Iterator<TimeValuePair> getIterator(){
        checkInitialized();
        return sortedTimeValuePairList.iterator();
    }

    @Override
    public boolean isEmpty() {
        checkInitialized();
        return sortedTimeValuePairList.size() == 0;
    }
}
