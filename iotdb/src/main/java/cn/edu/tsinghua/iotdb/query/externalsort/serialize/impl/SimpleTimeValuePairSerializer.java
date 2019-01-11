package cn.edu.tsinghua.iotdb.query.externalsort.serialize.impl;

import cn.edu.tsinghua.iotdb.query.externalsort.serialize.TimeValuePairSerializer;
import cn.edu.tsinghua.iotdb.utils.TimeValuePair;

import java.io.*;


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
