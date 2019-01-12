package org.apache.iotdb.tsfile.write.writer;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * a TsFileOutput implementation with FileOutputStream. If the file is not existed, it will be created.
 * Otherwise the file will be written from position 0.
 */
public class DefaultTsFileOutput implements TsFileOutput{

    FileOutputStream outputStream;

    public DefaultTsFileOutput(File file) throws FileNotFoundException {
        this.outputStream = new FileOutputStream(file);
    }

    public DefaultTsFileOutput(FileOutputStream outputStream) throws FileNotFoundException {
        this.outputStream = outputStream;
    }

    @Override
    public void write(byte[] b) throws IOException {
        outputStream.write(b);
    }

    @Override
    public void write(ByteBuffer b) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public long getPosition() throws IOException {
        return outputStream.getChannel().position();
    }

    @Override
    public void close() throws IOException {
        outputStream.close();
    }

    @Override
    public OutputStream wrapAsStream() throws IOException {
        return outputStream;
    }

    @Override
    public void flush() throws IOException {
        this.outputStream.flush();
    }

    @Override
    public void truncate(long position) throws IOException {
        outputStream.getChannel().truncate(position);
    }

}
