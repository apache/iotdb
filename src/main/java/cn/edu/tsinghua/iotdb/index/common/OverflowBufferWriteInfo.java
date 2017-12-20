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

    private DynamicOneColumnData insert;

    private DynamicOneColumnData update;

    // deleteUntil means data deleted when timestamp <= deleteUntil
    private long deleteUntil;

    private long bufferWriteBeginTime;

    public OverflowBufferWriteInfo(DynamicOneColumnData insert, DynamicOneColumnData update, long deleteUntil, long bufferWriteBeginTime) {
        this.insert = insert;
        this.update = update;
        this.deleteUntil = deleteUntil;
        this.bufferWriteBeginTime = bufferWriteBeginTime;
    }

    public List<Pair<Long, Long>> getInsertOrUpdateIntervals() {
        List<Pair<Long, Long>> insertIntervals = new ArrayList<>();
        if (insert != null) {
            for (int i = 0; i < insert.timeLength; i++) {
                insertIntervals.add(new Pair<>(insert.getTime(i), insert.getTime(i)));
            }
        }
        if (bufferWriteBeginTime < Long.MAX_VALUE) {
            insertIntervals.add(new Pair<>(bufferWriteBeginTime, Long.MAX_VALUE));
            insertIntervals = IntervalUtils.sortAndMergePair(insertIntervals);
        }
        List<Pair<Long, Long>> updateIntervals = new ArrayList<>();
        if (update != null) {
            for (int i = 0; i < update.timeLength; i += 2) {
                updateIntervals.add(new Pair<>(update.getTime(i), update.getTime(i + 1)));
            }
        }
        return IntervalUtils.union(insertIntervals, updateIntervals);
    }

    public long getDeleteUntil() {
        return deleteUntil;
    }
}
