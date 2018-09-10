package cn.edu.tsinghua.iotdb.engine.memtable;

import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;

/**
 * Created by zhangjinrui on 2018/1/26.
 */
//0910:实现了时间戳的比较
public class TimeValuePairInMemTable extends TimeValuePair implements Comparable<TimeValuePairInMemTable> {

    public TimeValuePairInMemTable(long timestamp, TsPrimitiveType value) {
        super(timestamp, value);
    }

    @Override
    public int compareTo(TimeValuePairInMemTable o) {
        return Long.compare(this.getTimestamp(), o.getTimestamp());
    }

    @Override
    public boolean equals(Object object) {
        if (object == null || !(object instanceof TimeValuePairInMemTable))
            return false;
        TimeValuePairInMemTable o = (TimeValuePairInMemTable) object;
        return o.getTimestamp() == this.getTimestamp();
    }
}