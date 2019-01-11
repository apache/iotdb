//package cn.edu.tsinghua.iotdb.query.externalsort;
//
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//
///**
// * This class represents an external sort job. Every job will use a separated directory.
// */
//public class ExternalSortJob {
//    private long jobId;
//    private List<ExternalSortJobPart> partList;
//
//    public ExternalSortJob(long jobId, List<ExternalSortJobPart> partList) {
//        this.jobId = jobId;
//        this.partList = partList;
//    }
//
//    public List<PrioritySeriesReader> executeWithGlobalTimeFilter() throws IOException {
//        List<PrioritySeriesReader> readers = new ArrayList<>();
//        for (ExternalSortJobPart part : partList) {
//            readers.add(part.executeWithGlobalTimeFilter());
//        }
//        return readers;
//    }
//}
