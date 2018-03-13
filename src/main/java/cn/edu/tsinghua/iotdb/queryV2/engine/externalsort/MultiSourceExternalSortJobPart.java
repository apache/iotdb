package cn.edu.tsinghua.iotdb.queryV2.engine.externalsort;

import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityTimeValuePairReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhangjinrui on 2018/1/20.
 */
public class MultiSourceExternalSortJobPart extends ExternalSortJobPart {
    private String tmpFilePath;
    private List<ExternalSortJobPart> source;

    public MultiSourceExternalSortJobPart(String tmpFilePath, List<ExternalSortJobPart> source) {
        super(ExternalSortJobPartType.MULTIPLE_SOURCE);
        this.source = source;
        this.tmpFilePath = tmpFilePath;
    }

    public MultiSourceExternalSortJobPart(String tmpFilePath, ExternalSortJobPart... externalSortJobParts) {
        super(ExternalSortJobPartType.MULTIPLE_SOURCE);
        source = new ArrayList<>();
        for (ExternalSortJobPart externalSortJobPart : externalSortJobParts) {
            source.add(externalSortJobPart);
        }
        this.tmpFilePath = tmpFilePath;
    }

    @Override
    public PriorityTimeValuePairReader execute() throws IOException {
        List<PriorityTimeValuePairReader> priorityTimeValuePairReaders = new ArrayList<>();
        for (ExternalSortJobPart part : source) {
            priorityTimeValuePairReaders.add(part.execute());
        }
        LineMerger merger = new LineMerger(tmpFilePath);
        return merger.merge(priorityTimeValuePairReaders);
    }
}
