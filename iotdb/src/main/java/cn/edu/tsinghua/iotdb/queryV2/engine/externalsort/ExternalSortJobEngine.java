package cn.edu.tsinghua.iotdb.queryV2.engine.externalsort;

import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityTimeValuePairReader;

import java.io.IOException;
import java.util.List;

/**
 * Created by zhangjinrui on 2018/1/20.
 */
public interface ExternalSortJobEngine {

    /**
     * Receive a list of TimeValuePairReaders and judge whether it should be processed using external sort.
     * If needed, do the merge sort for all TimeValuePairReaders using specific strategy.
     * @param timeValuePairReaderList A list include a set of TimeValuePairReaders
     * @return
     */
    List<PriorityTimeValuePairReader> execute(List<PriorityTimeValuePairReader> timeValuePairReaderList) throws IOException;

    /**
     * Create an external sort job which contains many parts.
     * @param timeValuePairReaderList
     * @return
     */
    ExternalSortJob createJob(List<PriorityTimeValuePairReader> timeValuePairReaderList);

}
