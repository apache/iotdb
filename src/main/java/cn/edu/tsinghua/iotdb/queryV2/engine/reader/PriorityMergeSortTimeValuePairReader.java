package cn.edu.tsinghua.iotdb.queryV2.engine.reader;

import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityTimeValuePairReader.Priority;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.TimeValuePairReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Created by zhangjinrui on 2018/1/11.
 */
public class PriorityMergeSortTimeValuePairReader implements TimeValuePairReader, SeriesReader {

    private List<PriorityTimeValuePairReader> readerList;
    private PriorityQueue<Element> heap;

    public PriorityMergeSortTimeValuePairReader(PriorityTimeValuePairReader... readers) throws IOException {
        readerList = new ArrayList<>();
        for (int i = 0; i < readers.length; i++) {
            readerList.add(readers[i]);
        }
        init();
    }

    public PriorityMergeSortTimeValuePairReader(List<PriorityTimeValuePairReader> readerList) throws IOException {
        this.readerList = readerList;
        init();
    }

    private void init() throws IOException {
        heap = new PriorityQueue<>();
        for (int i = 0; i < readerList.size(); i++) {
            if (readerList.get(i).hasNext()) {
                heap.add(new Element(i, readerList.get(i).next(), readerList.get(i).getPriority()));
            }
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        return heap.size() > 0;
    }

    @Override
    public TimeValuePair next() throws IOException {
        Element top = heap.peek();
        updateHeap(top);
        return top.timeValuePair;
    }

    private void updateHeap(Element top) throws IOException {
        while (heap.size() > 0 && heap.peek().timeValuePair.getTimestamp() == top.timeValuePair.getTimestamp()) {
            Element e = heap.poll();
            PriorityTimeValuePairReader priorityTimeValuePairReader = readerList.get(e.index);
            if (priorityTimeValuePairReader.hasNext()) {
                heap.add(new Element(e.index, priorityTimeValuePairReader.next(), priorityTimeValuePairReader.getPriority()));
            }
        }
    }

    @Override
    public void skipCurrentTimeValuePair() throws IOException {
        if (hasNext()) {
            next();
        }
    }

    @Override
    public void close() throws IOException {
        for (TimeValuePairReader timeValuePairReader : readerList) {
            timeValuePairReader.close();
        }
    }

    private class Element implements Comparable<Element> {
        int index;
        TimeValuePair timeValuePair;
        Priority priority;

        public Element(int index, TimeValuePair timeValuePair, Priority priority) {
            this.index = index;
            this.timeValuePair = timeValuePair;
            this.priority = priority;
        }

        @Override
        public int compareTo(Element o) {
            return this.timeValuePair.getTimestamp() > o.timeValuePair.getTimestamp() ? 1 :
                    this.timeValuePair.getTimestamp() < o.timeValuePair.getTimestamp() ? -1 :
                            o.priority.compareTo(this.priority);
        }
    }
}
