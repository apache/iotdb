package cn.edu.tsinghua.iotdb.engine.memtable;

import cn.edu.tsinghua.tsfile.utils.Binary;


public interface IWritableMemChunk extends TimeValuePairSorter{

    void putLong(long t, long v);

    void putInt(long t, int v);

    void putFloat(long t, float v);

    void putDouble(long t, double v);

    void putBinary(long t, Binary v);

    void putBoolean(long t, boolean v);

    void write(long insertTime, String insertValue);

    void reset();

    int count();
}
