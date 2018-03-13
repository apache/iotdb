package cn.edu.tsinghua.iotdb.engine.memtable;

import cn.edu.tsinghua.iotdb.utils.PrimitiveArrayList;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

/**
 * Created by zhangjinrui on 2018/1/26.
 */
public class PrimitiveMemSeriesLazyIterable implements Iterable<TimeValuePair>{

    private TSDataType dataType;
    private PrimitiveArrayList list;

    public PrimitiveMemSeriesLazyIterable(TSDataType dataType, PrimitiveArrayList list) {
        this.dataType = dataType;
        this.list = list;
    }

    @Override
    public Iterator<TimeValuePair> iterator() {
        int length = list.size();
        TreeMap<Long, TsPrimitiveType> treeMap = new TreeMap<>();
        for (int i = 0; i < length; i++) {
            treeMap.put(list.getTimestamp(i), TsPrimitiveType.getByType(dataType, list.getValue(i)));
        }
        List<TimeValuePair> ret = new ArrayList<>();
        treeMap.forEach((k, v) -> {
            ret.add(new TimeValuePairInMemTable(k, v));
        });
        return ret.iterator();
    }
}
