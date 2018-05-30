package cn.edu.tsinghua.iotdb.queryV2.engine.reader;

import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReaderByTimeStamp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.sort;

public class PriorityMergeSortTimeValuePairReaderByTimestamp implements SeriesReaderByTimeStamp {

    private List<PriorityTimeValuePairReaderByTimestamp> readerList;
    private PriorityTimeValuePairReader timeValuePairReader;


    public PriorityMergeSortTimeValuePairReaderByTimestamp(PriorityTimeValuePairReaderByTimestamp... readers){
        readerList = new ArrayList<>();
        for (int i = 0; i < readers.length; i++) {
            readerList.add(readers[i]);
        }
        sort(readerList, Collections.reverseOrder());
    }

    public PriorityMergeSortTimeValuePairReaderByTimestamp(List<PriorityTimeValuePairReaderByTimestamp> readers){
        readerList = new ArrayList<>();
        for (int i = 0; i < readers.size(); i++) {
            readerList.add(readers.get(i));
        }
        sort(readerList, Collections.reverseOrder());
    }

    @Override
    public void setCurrentTimestamp(long timestamp) {
        for (PriorityTimeValuePairReaderByTimestamp priorityTimeValuePairReaderByTimestamp : readerList) {
            priorityTimeValuePairReaderByTimestamp.setCurrentTimestamp(timestamp);
        }
    }


    @Override
    public boolean hasNext() throws IOException {
        for (PriorityTimeValuePairReaderByTimestamp priorityTimeValuePairReaderByTimestamp : readerList) {
            if(priorityTimeValuePairReaderByTimestamp.hasNext()){
                timeValuePairReader = priorityTimeValuePairReaderByTimestamp;
                return true;
            }

        }
        return false;
    }

    @Override
    public TimeValuePair next() throws IOException {
        return timeValuePairReader.next();
    }

    @Override
    public void skipCurrentTimeValuePair() throws IOException {
        next();
    }

    @Override
    public void close() throws IOException {
        for (PriorityTimeValuePairReaderByTimestamp priorityTimeValuePairReaderByTimestamp : readerList) {
            priorityTimeValuePairReaderByTimestamp.close();
        }
    }

}
