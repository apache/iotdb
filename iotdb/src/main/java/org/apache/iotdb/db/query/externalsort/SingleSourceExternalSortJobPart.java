//package org.apache.iotdb.db.query.externalsort;
//
//import org.apache.iotdb.db.query.reader.merge.PrioritySeriesReader;
//
//
//public class SingleSourceExternalSortJobPart extends ExternalSortJobPart {
//
//    private PrioritySeriesReader timeValuePairReader;
//
//    public SingleSourceExternalSortJobPart(PrioritySeriesReader timeValuePairReader) {
//        super(ExternalSortJobPartType.SINGLE_SOURCE);
//        this.timeValuePairReader = timeValuePairReader;
//    }
//
//    @Override
//    public PrioritySeriesReader executeWithGlobalTimeFilter() {
//        return this.timeValuePairReader;
//    }
//}
