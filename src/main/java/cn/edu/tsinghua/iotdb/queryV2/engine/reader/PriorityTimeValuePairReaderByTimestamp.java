package cn.edu.tsinghua.iotdb.queryV2.engine.reader;

import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReaderByTimeStamp;

import java.io.IOException;

public class PriorityTimeValuePairReaderByTimestamp extends PriorityTimeValuePairReader
        implements SeriesReaderByTimeStamp, Comparable<PriorityTimeValuePairReaderByTimestamp>  {

    public PriorityTimeValuePairReaderByTimestamp(SeriesReaderByTimeStamp seriesReader, Priority priority){
        super(seriesReader, priority);
    }

    @Override
    public int compareTo(PriorityTimeValuePairReaderByTimestamp o) {
        return this.priority.compareTo(o.getPriority());
    }

    @Override
    public TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException {
        return ((SeriesReaderByTimeStamp)seriesReader).getValueInTimestamp(timestamp);
    }
}
