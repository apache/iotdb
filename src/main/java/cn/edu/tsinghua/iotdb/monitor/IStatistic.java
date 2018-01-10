package cn.edu.tsinghua.iotdb.monitor;

import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;


public interface IStatistic {
    /**
     * @return An HashMap that contains the Module path like: root.stats.write.global,
     * and its value as TSRecord format contains all the statistics measurement
     */
    HashMap<String, TSRecord> getAllStatisticsValue();

    /**
     * A method need to provide an HashMap to statMonitor to register statistics info params
     * HashMap key is the param storage path like "root.stats.xxx.statisticsParams", value is
     * its dataType: MonitorConstants.DataType
     */
    void registStatMetadata();

    /**
     * Get all the Module's statistics params time-series path
     *
     * @return a list of string like "root.stats.xxx.statisticsParams",
     * the name is the statistics name need to store
     */
    List<String> getAllPathForStatistic();

    /**
     *
     * @return a HashMap contains the name and values of the statistics parameters
     */
    HashMap<String, AtomicLong> getStatParamsHashMap();
}
