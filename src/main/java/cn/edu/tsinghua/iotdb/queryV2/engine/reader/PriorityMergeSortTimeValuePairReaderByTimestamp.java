package cn.edu.tsinghua.iotdb.queryV2.engine.reader;

import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReaderByTimeStamp;

import java.io.IOException;
import java.util.*;

import static java.util.Collections.sort;

public class PriorityMergeSortTimeValuePairReaderByTimestamp implements SeriesReaderByTimeStamp {

    private List<PriorityTimeValuePairReaderByTimestamp> readerList;
    private SeriesReaderByTimeStamp currentTimeValuePairReader;


    public PriorityMergeSortTimeValuePairReaderByTimestamp(PriorityTimeValuePairReaderByTimestamp... readers){
        readerList = new ArrayList<>();

        int size = readers.length;
        if(size < 1){
            return;
        }
        //sort readers by priority using PriorityQueue
        Queue<PriorityTimeValuePairReaderByTimestamp> priorityQueue = new PriorityQueue<>(readers.length);
        for (int i = 0; i < size; i++) {
            priorityQueue.add(readers[i]);
        }
        for(int i = 0; i < size; i++){
            readerList.add(priorityQueue.poll());
        }
    }

    public PriorityMergeSortTimeValuePairReaderByTimestamp(List<PriorityTimeValuePairReaderByTimestamp> readers){
        readerList = new ArrayList<>();

        int size = readers.size();
        if(size < 1){
            return;
        }

        //sort readers by priority using PriorityQueue
        Queue<PriorityTimeValuePairReaderByTimestamp> priorityQueue = new PriorityQueue<>(readers.size());
        for (int i = 0; i < size; i++) {
            priorityQueue.add(readers.get(i));
        }
        for(int i = 0; i < size; i++){
            readerList.add(priorityQueue.poll());
        }
    }


    @Override
    public boolean hasNext() throws IOException {
        for (PriorityTimeValuePairReaderByTimestamp priorityTimeValuePairReaderByTimestamp : readerList) {
            if(priorityTimeValuePairReaderByTimestamp.hasNext()){
                currentTimeValuePairReader = priorityTimeValuePairReaderByTimestamp;
                return true;
            }
        }
        return false;
    }

    @Override
    public TimeValuePair next() throws IOException {
        return currentTimeValuePairReader.next();
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

    @Override
    public TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException {
        TsPrimitiveType value = null;
        for (PriorityTimeValuePairReaderByTimestamp priorityTimeValuePairReaderByTimestamp : readerList) {
            value = priorityTimeValuePairReaderByTimestamp.getValueInTimestamp(timestamp);
            if(value != null){
                break;
            }

        }
        return value;
    }
}
