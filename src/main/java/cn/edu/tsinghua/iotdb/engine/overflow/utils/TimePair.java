package cn.edu.tsinghua.iotdb.engine.overflow.utils;


import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

/**
 * TimePair represents an overflow operation.
 *
 * @author CGF
 */

public class TimePair {

    public long s;  // start time
    public long e;  // end time
    public byte[] v; // value
    public OverflowOpType opType = null;
    public TSDataType dataType;
    public MergeStatus mergestatus;

    public TimePair(long s, long e) {
        this.s = s;
        this.e = e;
        this.v = null;
    }

    public TimePair(long s, long e, byte[] v) {
        this.s = s;
        this.e = e;
        this.v = v;
    }

    public TimePair(long s, long e, MergeStatus status) {
        this(s, e);
        this.mergestatus = status;
    }

    public TimePair(long s, long e, byte[] v, TSDataType dataType) {
        this(s, e, v);
        this.dataType = dataType;
    }

    public TimePair(long s, long e, byte[] v, OverflowOpType overflowOpType) {
        this(s, e, v);
        this.opType = overflowOpType;
    }

    public TimePair(long s, long e, byte[] v, OverflowOpType overflowOpType, TSDataType dataType) {
        this(s, e, v, overflowOpType);
        this.dataType = dataType;
    }

    public TimePair(long s, long e, byte[] v, OverflowOpType type, MergeStatus status) {
        this(s, e, v);
        this.opType = type;
        this.mergestatus = status;
    }

    /**
     * Set TimePair s = -1 and e = -1 means reset.
     */
    public void reset() {
        s = -1;
        e = -1;
        mergestatus = MergeStatus.DONE;
    }

    public String toString() {
        StringBuffer sb =  new StringBuffer().append(this.s).append(",").append(this.e);
        if (this.opType != null)
            sb.append(",").append(this.opType);
        return sb.toString();
    }
}

