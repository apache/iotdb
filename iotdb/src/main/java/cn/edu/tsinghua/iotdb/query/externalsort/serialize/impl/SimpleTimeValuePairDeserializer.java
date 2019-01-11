package cn.edu.tsinghua.iotdb.query.externalsort.serialize.impl;

import cn.edu.tsinghua.iotdb.query.externalsort.serialize.TimeValuePairDeserializer;
import cn.edu.tsinghua.iotdb.utils.TimeValuePair;

import java.io.*;


public class SimpleTimeValuePairDeserializer implements TimeValuePairDeserializer {

    private InputStream inputStream;
    private ObjectInputStream objectInputStream;
    private String tmpFilePath;

    public SimpleTimeValuePairDeserializer(String tmpFilePath) throws IOException {
        inputStream = new BufferedInputStream(new FileInputStream(tmpFilePath));
        objectInputStream = new ObjectInputStream(inputStream);
        this.tmpFilePath = tmpFilePath;
    }

    @Override
    public boolean hasNext() throws IOException {
        return inputStream.available() > 0;
    }

    @Override
    public TimeValuePair next() throws IOException {
        try {
            return (TimeValuePair) objectInputStream.readUnshared();
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

//    @Override
//    public void skipCurrentTimeValuePair() throws IOException {
//        next();
//    }

    /**
     * This method will delete
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        objectInputStream.close();
        File file = new File(tmpFilePath);
        if (!file.delete()) {
            throw new IOException("Delete external sort tmp file error. FilePath:" + tmpFilePath);
        }
    }
}
