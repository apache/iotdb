package cn.edu.tsinghua.iotdb.queryV2.engine.reader;

import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReaderByTimeStamp;

public class PriorityTimeValuePairReaderByTimestamp extends PriorityTimeValuePairReader
        implements SeriesReaderByTimeStamp, Comparable<PriorityTimeValuePairReaderByTimestamp>  {

    public PriorityTimeValuePairReaderByTimestamp(SeriesReaderByTimeStamp seriesReader, Priority priority){
        super(seriesReader, priority);
    }

    @Override
    public void setCurrentTimestamp(long timestamp) {
        ((SeriesReaderByTimeStamp)seriesReader).setCurrentTimestamp(timestamp);
    }

    @Override
    public int compareTo(PriorityTimeValuePairReaderByTimestamp o) {
        return this.priority.compareTo(o.getPriority());
    }
}
