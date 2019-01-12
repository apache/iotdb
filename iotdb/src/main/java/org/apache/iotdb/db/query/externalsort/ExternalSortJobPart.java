//package org.apache.iotdb.db.query.externalsort;
//
//import org.apache.iotdb.db.query.reader.merge.PrioritySeriesReader;
//
//import java.io.IOException;
//
//
//public abstract class ExternalSortJobPart {
//
//    private ExternalSortJobPartType type;
//
//    public ExternalSortJobPart (ExternalSortJobPartType type) {
//        this.type = type;
//    }
//
//    public abstract PrioritySeriesReader executeWithGlobalTimeFilter() throws IOException;
//
//    public ExternalSortJobPartType getType() {
//        return type;
//    }
//
//    public enum ExternalSortJobPartType {
//        SINGLE_SOURCE, MULTIPLE_SOURCE
//    }
//
//}
