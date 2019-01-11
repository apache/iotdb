//package cn.edu.tsinghua.iotdb.query.externalsort;
//
//import cn.edu.tsinghua.iotdb.query.reader.merge.PrioritySeriesReader;
//
//import java.io.IOException;
//import java.util.List;
//
//
//public interface ExternalSortJobEngine {
//
//    /**
//     * Receive a list of TimeValuePairReaders and judge whether it should be processed using external sort.
//     * If needed, do the merge sort for all TimeValuePairReaders using specific strategy.
//     * @param timeValuePairReaderList A list include a set of TimeValuePairReaders
//     * @return
//     */
//    List<PrioritySeriesReader> executeWithGlobalTimeFilter(List<PrioritySeriesReader> timeValuePairReaderList) throws IOException;
//
//    /**
//     * Create an external sort job which contains many parts.
//     * @param timeValuePairReaderList
//     * @return
//     */
//    ExternalSortJob createJob(List<PrioritySeriesReader> timeValuePairReaderList);
//
//}
