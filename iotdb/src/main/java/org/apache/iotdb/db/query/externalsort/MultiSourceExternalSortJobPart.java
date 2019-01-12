//package org.apache.iotdb.db.query.externalsort;
//
//import org.apache.iotdb.db.query.reader.merge.PrioritySeriesReader;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//
//
//public class MultiSourceExternalSortJobPart extends ExternalSortJobPart {
//    private String tmpFilePath;
//    private List<ExternalSortJobPart> source;
//
//    public MultiSourceExternalSortJobPart(String tmpFilePath, List<ExternalSortJobPart> source) {
//        super(ExternalSortJobPartType.MULTIPLE_SOURCE);
//        this.source = source;
//        this.tmpFilePath = tmpFilePath;
//    }
//
////    public MultiSourceExternalSortJobPart(String tmpFilePath, ExternalSortJobPart... externalSortJobParts) {
////        super(ExternalSortJobPartType.MULTIPLE_SOURCE);
////        source = new ArrayList<>();
////        for (ExternalSortJobPart externalSortJobPart : externalSortJobParts) {
////            source.add(externalSortJobPart);
////        }
////        this.tmpFilePath = tmpFilePath;
////    }
//
//    @Override
//    public PrioritySeriesReader executeWithGlobalTimeFilter() throws IOException {
//        List<PrioritySeriesReader> prioritySeriesReaders = new ArrayList<>();
//        for (ExternalSortJobPart part : source) {
//            prioritySeriesReaders.add(part.executeWithGlobalTimeFilter());
//        }
//        LineMerger merger = new LineMerger(tmpFilePath);
//        return merger.merge(prioritySeriesReaders);
//    }
//}
