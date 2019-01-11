//package cn.edu.tsinghua.iotdb.query.externalsort;
//
//import cn.edu.tsinghua.iotdb.query.reader.merge.PrioritySeriesReader;
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
