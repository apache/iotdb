package cn.edu.tsinghua.iotdb.engine.memtable;

import cn.edu.tsinghua.iotdb.engine.querycontext.RawSeriesChunk;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;

import java.util.Iterator;

/**
 * Created by zhangjinrui on 2018/1/21.
 */
public class SeriesChunkInMemTable implements RawSeriesChunk {
	
	private long maxTime;
	private long minTime;
	
	private TsPrimitiveType maxValue;
	private TsPrimitiveType minValue;
	private TSDataType dataType;
	private Iterable<TimeValuePair> values;
	private boolean isEmpty;
	
	public SeriesChunkInMemTable(boolean isEmpty){
		this.isEmpty = isEmpty;
	}
	
    public SeriesChunkInMemTable(long maxTime, long minTime, TsPrimitiveType maxValue, TsPrimitiveType minValue,
                                 TSDataType dataType, Iterable<TimeValuePair> values) {
		this.maxTime = maxTime;
		this.minTime = minTime;
		this.maxValue = maxValue;
		this.minValue = minValue;
		this.dataType = dataType;
		this.values = values;
	}

	@Override
    public TSDataType getDataType() {
        return dataType;
    }

    @Override
    public long getMaxTimestamp() {
        return maxTime;
    }

    @Override
    public long getMinTimestamp() {
        return minTime;
    }

    @Override
    public TsPrimitiveType getMaxValue() {
        return maxValue;
    }

    @Override
    public TsPrimitiveType getMinValue() {
        return minValue;
    }

    @Override
    public Iterator<TimeValuePair> getIterator() {
        return values.iterator();
    }

	@Override
	public boolean isEmpty() {
		return isEmpty;
	}
}
