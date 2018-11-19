package cn.edu.tsinghua.iotdb.monitor;

import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;


public interface IStatistic {
    /**
     * @return A HashMap that contains the module path like: root.stats.write.global,
     * and its value is TSRecord format contains all statistics measurement
     */
    HashMap<String, TSRecord> getAllStatisticsValue();

    /**
     * A method to register statistics info
     */
    void registStatMetadata();

    /**
     * Get all module's statistics parameters as time-series path
     *
     * @return a list of string like "root.stats.xxx.statisticsParams",
     */
    List<String> getAllPathForStatistic();

    /**
     *
     * @return a HashMap contains the names and values of the statistics parameters
     */
    HashMap<String, AtomicLong> getStatParamsHashMap();
}
