package cn.edu.tsinghua.iotdb.queryV2.engine.reader;

import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReaderByTimeStamp;

import java.io.IOException;
import java.util.*;

import static java.util.Collections.sort;

public class PriorityMergeSortTimeValuePairReaderByTimestamp extends PriorityMergeSortTimeValuePairReader<PriorityTimeValuePairReaderByTimestamp>
        implements SeriesReaderByTimeStamp {

    private long currentTimestamp;
    private boolean hasCachedTimeValuePair;
    private TimeValuePair cachedTimeValuePair;

    public PriorityMergeSortTimeValuePairReaderByTimestamp(PriorityTimeValuePairReaderByTimestamp... readers) throws IOException {
       super(readers);
       currentTimestamp = Long.MIN_VALUE;
       hasCachedTimeValuePair = false;
    }

    public PriorityMergeSortTimeValuePairReaderByTimestamp(List<PriorityTimeValuePairReaderByTimestamp> readers) throws IOException {
        super(readers);
        currentTimestamp = Long.MIN_VALUE;
        hasCachedTimeValuePair = false;
    }

    @Override
    public boolean hasNext() throws IOException {
        if(hasCachedTimeValuePair && cachedTimeValuePair.getTimestamp() >= currentTimestamp){
            return true;
        }
        while (heap.size()>0){
            Element top = heap.peek();
            updateHeap(top);
            if(top.timeValuePair.getTimestamp() >= currentTimestamp){
                hasCachedTimeValuePair = true;
                cachedTimeValuePair = top.timeValuePair;
                return true;
            }
        }
        return false;
    }

    @Override
    public TimeValuePair next() throws IOException {
        if(hasCachedTimeValuePair){
            hasCachedTimeValuePair = false;
            return cachedTimeValuePair;
        }

        Element top = heap.peek();
        updateHeap(top);
        return top.timeValuePair;
    }

    private void updateHeap(Element top) throws IOException {
        while (heap.size() > 0 && heap.peek().timeValuePair.getTimestamp() == top.timeValuePair.getTimestamp()) {
            Element e = heap.poll();
            PriorityTimeValuePairReaderByTimestamp priorityTimeValuePairReader = readerList.get(e.index);

            if(currentTimestamp > top.timeValuePair.getTimestamp()){
                TsPrimitiveType value = priorityTimeValuePairReader.getValueInTimestamp(currentTimestamp);
                if(value != null){
                    heap.add(new Element(e.index, new TimeValuePair(currentTimestamp, value), priorityTimeValuePairReader.getPriority()));
                }
                else {
                    //judge if priorityTimeValuePairReader has a timeValuePair whose time > currentTimestamp when it doesn't has a timeValuePair in currentTimestamp
                    if (priorityTimeValuePairReader.hasNext()) {
                        heap.add(new Element(e.index, priorityTimeValuePairReader.next(), priorityTimeValuePairReader.getPriority()));
                    }
                }
            }
            else {
                if (priorityTimeValuePairReader.hasNext()) {
                    heap.add(new Element(e.index, priorityTimeValuePairReader.next(), priorityTimeValuePairReader.getPriority()));
                }
            }
        }
    }

    @Override
    public TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException {
        currentTimestamp = timestamp;
        if(hasNext()){
            cachedTimeValuePair = next();
            if(cachedTimeValuePair.getTimestamp() == timestamp){
                return cachedTimeValuePair.getValue();
            }
            else if(cachedTimeValuePair.getTimestamp() > timestamp){
                hasCachedTimeValuePair = true;
            }
        }
        return null;
    }
}
