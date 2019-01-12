package org.apache.iotdb.db.engine.memtable;


import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TsPrimitiveType;

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