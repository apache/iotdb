package cn.edu.tsinghua.iotdb.queryV2.engine.externalsort;

import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityTimeValuePairReader;

import java.io.IOException;

/**
 * Created by zhangjinrui on 2018/1/20.
 */
public abstract class ExternalSortJobPart {

    private ExternalSortJobPartType type;

    public ExternalSortJobPart (ExternalSortJobPartType type) {
        this.type = type;
    }

    public abstract PriorityTimeValuePairReader execute() throws IOException;

    public ExternalSortJobPartType getType() {
        return type;
    }

    public enum ExternalSortJobPartType {
        SINGLE_SOURCE, MULTIPLE_SOURCE
    }

}
