package cn.edu.tsinghua.iotdb.queryV2.engine.externalsort;

import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityTimeValuePairReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.TimeValuePairReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class represents an external sort job. Every job will use a separated directory.
 * Created by zhangjinrui on 2018/1/20.
 */
public class ExternalSortJob {
    private long jobId;
    private List<ExternalSortJobPart> partList;

    public ExternalSortJob(long jobId, List<ExternalSortJobPart> partList) {
        this.jobId = jobId;
        this.partList = partList;
    }

    public List<PriorityTimeValuePairReader> execute() throws IOException {
        List<PriorityTimeValuePairReader> readers = new ArrayList<>();
        for (ExternalSortJobPart part : partList) {
            readers.add(part.execute());
        }
        return readers;
    }
}
