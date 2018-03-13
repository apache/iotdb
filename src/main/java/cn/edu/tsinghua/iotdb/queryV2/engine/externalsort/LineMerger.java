package cn.edu.tsinghua.iotdb.queryV2.engine.externalsort;

import cn.edu.tsinghua.iotdb.queryV2.engine.externalsort.serialize.TimeValuePairDeserializer;
import cn.edu.tsinghua.iotdb.queryV2.engine.externalsort.serialize.TimeValuePairSerializer;
import cn.edu.tsinghua.iotdb.queryV2.engine.externalsort.serialize.impl.FixLengthTimeValuePairDeserializer;
import cn.edu.tsinghua.iotdb.queryV2.engine.externalsort.serialize.impl.FixLengthTimeValuePairSerializer;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityMergeSortTimeValuePairReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityTimeValuePairReader;

import java.io.IOException;
import java.util.List;

/**
 * Created by zhangjinrui on 2018/1/20.
 */
public class LineMerger {

    private String tmpFilePath;

    public LineMerger(String tmpFilePath) {
        this.tmpFilePath = tmpFilePath;
    }

    public PriorityTimeValuePairReader merge(List<PriorityTimeValuePairReader> priorityTimeValuePairReaders) throws IOException {
        TimeValuePairSerializer serializer = new FixLengthTimeValuePairSerializer(tmpFilePath);
        PriorityMergeSortTimeValuePairReader reader = new PriorityMergeSortTimeValuePairReader(priorityTimeValuePairReaders);
        while (reader.hasNext()) {
            serializer.write(reader.next());
        }
        reader.close();
        serializer.close();
        TimeValuePairDeserializer deserializer = new FixLengthTimeValuePairDeserializer(tmpFilePath);
        return new PriorityTimeValuePairReader(deserializer, priorityTimeValuePairReaders.get(0).getPriority());
    }
}
