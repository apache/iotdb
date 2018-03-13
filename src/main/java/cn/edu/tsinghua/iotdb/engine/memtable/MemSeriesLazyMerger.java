package cn.edu.tsinghua.iotdb.engine.memtable;

import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhangjinrui on 2018/1/26.
 */
public class MemSeriesLazyMerger implements TimeValuePairSorter{

    private List<IMemSeries> memSeriesList;

    public MemSeriesLazyMerger() {
        memSeriesList = new ArrayList<>();
    }

    public MemSeriesLazyMerger(IMemSeries... memSerieses) {
        this();
        for (IMemSeries memSeries : memSerieses) {
            memSeriesList.add(memSeries);
        }
    }

    /**
     * IMPORTANT: Please ensure that the minimum timestamp of added {@link IMemSeries} is larger than
     * any timestamps of the IMemSeries already added in.
     * @param series
     */
    public void addMemSeries(IMemSeries series) {
        memSeriesList.add(series);
    }

    public List<TimeValuePair> getSortedTimeValuePairList() {
        if (memSeriesList.size() == 0) {
            return new ArrayList<>();
        } else {
            List<TimeValuePair> ret = memSeriesList.get(0).getSortedTimeValuePairList();
            for (int i = 1; i < memSeriesList.size(); i++) {
                ret.addAll(memSeriesList.get(i).getSortedTimeValuePairList());
            }
            return ret;
        }
    }
}
