package cn.edu.tsinghua.iotdb.queryV2.externalsort;

import cn.edu.tsinghua.iotdb.queryV2.engine.externalsort.serialize.TimeValuePairDeserializer;
import cn.edu.tsinghua.iotdb.queryV2.engine.externalsort.serialize.TimeValuePairSerializer;
import cn.edu.tsinghua.iotdb.queryV2.engine.externalsort.serialize.impl.FixLengthTimeValuePairDeserializer;
import cn.edu.tsinghua.iotdb.queryV2.engine.externalsort.serialize.impl.FixLengthTimeValuePairSerializer;
import cn.edu.tsinghua.iotdb.queryV2.engine.externalsort.serialize.impl.SimpleTimeValuePairDeserializer;
import cn.edu.tsinghua.iotdb.queryV2.engine.externalsort.serialize.impl.SimpleTimeValuePairSerializer;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * Created by zhangjinrui on 2018/1/20.
 */
public class SimpleTimeValuePairSerializerTest {

    private enum Type {
        SIMPLE, FIX_LENGTH
    }

    @Test
    public void testSIMPLE() throws IOException, ClassNotFoundException {
        String rootPath = "d1";
        String filePath = rootPath + "/d2/d3/tmpFile1";
        int count = 10000;
        testReadWrite(genTimeValuePairs(count), count, rootPath, filePath, Type.SIMPLE);
    }

    @Test
    public void testFIX_LENGTH() throws IOException, ClassNotFoundException {
        String rootPath = "tmpFile2";
        String filePath = rootPath;
        int count = 10000;
        testReadWrite(genTimeValuePairs(count, TSDataType.BOOLEAN), count, rootPath, filePath, Type.FIX_LENGTH);
        testReadWrite(genTimeValuePairs(count, TSDataType.INT32), count, rootPath, filePath, Type.FIX_LENGTH);
        testReadWrite(genTimeValuePairs(count, TSDataType.INT64), count, rootPath, filePath, Type.FIX_LENGTH);
        testReadWrite(genTimeValuePairs(count, TSDataType.FLOAT), count, rootPath, filePath, Type.FIX_LENGTH);
        testReadWrite(genTimeValuePairs(count, TSDataType.DOUBLE), count, rootPath, filePath, Type.FIX_LENGTH);
        testReadWrite(genTimeValuePairs(count, TSDataType.TEXT), count, rootPath, filePath, Type.FIX_LENGTH);
    }

    private void testReadWrite(TimeValuePair[] timeValuePairs, int count, String rootPath, String filePath, Type type) throws IOException, ClassNotFoundException {
        TimeValuePairSerializer serializer = null;
        if (type == Type.SIMPLE) {
            serializer = new SimpleTimeValuePairSerializer(filePath);
        } else if (type == Type.FIX_LENGTH) {
            serializer = new FixLengthTimeValuePairSerializer(filePath);
        }

        for (TimeValuePair timeValuePair : timeValuePairs) {
            serializer.write(timeValuePair);
        }
        serializer.close();

        TimeValuePairDeserializer deserializer = null;
        if (type == Type.SIMPLE) {
            deserializer = new SimpleTimeValuePairDeserializer(filePath);
        } else if (type == Type.FIX_LENGTH) {
            deserializer = new FixLengthTimeValuePairDeserializer(filePath);
        }

        int idx = 0;
        while (deserializer.hasNext()) {
            TimeValuePair timeValuePair = deserializer.next();
            Assert.assertEquals(timeValuePairs[idx].getValue(), timeValuePair.getValue());
            Assert.assertEquals(timeValuePairs[idx].getTimestamp(), timeValuePair.getTimestamp());
            idx++;
        }
        Assert.assertEquals(count, idx);
        deserializer.close();
        deleteFileRecursively(new File(rootPath));
    }

    private void deleteFileRecursively(File file) throws IOException {
        if (!file.exists()) {
            return;
        }
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                deleteFileRecursively(f);
            }
        }
        if (!file.delete())
            throw new IOException("Failed to delete file: " + file);
    }

    private TimeValuePair[] genTimeValuePairs(int count) {
        TimeValuePair[] timeValuePairs = new TimeValuePair[count];
        for (int i = 0; i < count; i++) {
            timeValuePairs[i] = new TimeValuePair(i, new TsPrimitiveType.TsInt(i));
        }
        return timeValuePairs;
    }

    private TimeValuePair[] genTimeValuePairs(int count, TSDataType dataType) {
        TimeValuePair[] timeValuePairs = new TimeValuePair[count];
        for (int i = 0; i < count; i++) {
            switch (dataType) {
                case BOOLEAN:
                    timeValuePairs[i] = new TimeValuePair(i, new TsPrimitiveType.TsBoolean(i % 2 == 0 ? true : false));
                    break;
                case INT32:
                    timeValuePairs[i] = new TimeValuePair(i, new TsPrimitiveType.TsInt(i));
                    break;
                case INT64:
                    timeValuePairs[i] = new TimeValuePair(i, new TsPrimitiveType.TsLong(i));
                    break;
                case FLOAT:
                    timeValuePairs[i] = new TimeValuePair(i, new TsPrimitiveType.TsFloat(i + 0.1f));
                    break;
                case DOUBLE:
                    timeValuePairs[i] = new TimeValuePair(i, new TsPrimitiveType.TsDouble(i + 0.12));
                    break;
                case TEXT:
                    timeValuePairs[i] = new TimeValuePair(i, new TsPrimitiveType.TsBinary(new Binary(String.valueOf(i))));
                    break;
            }
        }
        return timeValuePairs;
    }

}
