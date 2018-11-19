package cn.edu.tsinghua.iotdb.engine.memtable;

import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;

import java.util.List;

/**
 * Created by zhangjinrui on 2018/1/26.
 */
public interface TimeValuePairSorter {

    /**
     * @return a List which contains all distinct {@link TimeValuePair}s in ascending order by timestamp.
     */
    List<TimeValuePair> getSortedTimeValuePairList();
}
