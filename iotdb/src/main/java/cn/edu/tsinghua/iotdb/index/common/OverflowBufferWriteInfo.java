package cn.edu.tsinghua.iotdb.index.common;

import cn.edu.fudan.dsm.kvmatch.iotdb.utils.IntervalUtils;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

import java.util.ArrayList;
import java.util.List;

/**
 * The class is used for index query, storing overflow data and buffer-write information separately.
 *
 * @author CGF, Jiaye Wu
 */
public class OverflowBufferWriteInfo {

    private List<Pair<Long, Long>> insertOrUpdateIntervals;

    // deleteUntil means data deleted when timestamp <= deleteUntil
    private long deleteUntil;

    public OverflowBufferWriteInfo(List<Pair<Long, Long>> insertOrUpdateIntervals, long deleteUntil) {
        this.insertOrUpdateIntervals = insertOrUpdateIntervals;
        this.deleteUntil = deleteUntil;
    }

    public List<Pair<Long, Long>> getInsertOrUpdateIntervals() {
        return insertOrUpdateIntervals;
    }

    public long getDeleteUntil() {
        return deleteUntil;
    }
}
