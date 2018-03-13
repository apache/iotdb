package cn.edu.tsinghua.iotdb.queryV2.engine.externalsort.serialize.impl;

import cn.edu.tsinghua.iotdb.queryV2.engine.externalsort.serialize.TimeValuePairSerializer;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;

import java.io.*;

/**
 * Created by zhangjinrui on 2018/1/20.
 */
public class SimpleTimeValuePairSerializer implements TimeValuePairSerializer {

    private ObjectOutputStream objectOutputStream;

    public SimpleTimeValuePairSerializer(String tmpFilePath) throws IOException {
        checkPath(tmpFilePath);
        objectOutputStream = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(tmpFilePath)));
    }

    private void checkPath(String tmpFilePath) throws IOException {
        File file = new File(tmpFilePath);
        if (file.exists()) {
            file.delete();
        }
        if (file.getParentFile() != null) {
            file.getParentFile().mkdirs();
        }
        file.createNewFile();
    }

    @Override
    public void write(TimeValuePair timeValuePair) throws IOException {
        objectOutputStream.writeUnshared(timeValuePair);
    }

    @Override
    public void close() throws IOException {
        objectOutputStream.close();
    }
}
