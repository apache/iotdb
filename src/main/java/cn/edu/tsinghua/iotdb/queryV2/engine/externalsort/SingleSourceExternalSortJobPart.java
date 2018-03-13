package cn.edu.tsinghua.iotdb.queryV2.engine.externalsort;

import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityTimeValuePairReader;

/**
 * Created by zhangjinrui on 2018/1/20.
 */
public class SingleSourceExternalSortJobPart extends ExternalSortJobPart {

    private PriorityTimeValuePairReader timeValuePairReader;

    public SingleSourceExternalSortJobPart(PriorityTimeValuePairReader timeValuePairReader) {
        super(ExternalSortJobPartType.SINGLE_SOURCE);
        this.timeValuePairReader = timeValuePairReader;
    }

    @Override
    public PriorityTimeValuePairReader execute() {
        return this.timeValuePairReader;
    }
}
