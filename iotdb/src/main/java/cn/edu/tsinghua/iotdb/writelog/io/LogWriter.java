package cn.edu.tsinghua.iotdb.writelog.io;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.zip.CRC32;

public class LogWriter implements ILogWriter {

    private File logFile;
    private FileChannel outputStream;
    private CRC32 checkSummer = new CRC32();

    public LogWriter(String logFilePath) {
        logFile = new File(logFilePath);
    }

    @Override
    public void write(List<byte[]> logCache) throws IOException {
        if (outputStream == null)
            outputStream = new FileOutputStream(logFile, true).getChannel();
        int totalSize = 0;
        for (byte[] bytes : logCache) {
            totalSize += 4 + 8 + bytes.length;
        }
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        for (byte[] bytes : logCache) {
            buffer.putInt(bytes.length);
            checkSummer.reset();
            checkSummer.update(bytes);
            buffer.putLong(checkSummer.getValue());
            buffer.put(bytes);
        }
        buffer.flip();
        outputStream.write(buffer);
        outputStream.force(true);
    }

    @Override
    public void close() throws IOException {
        if (outputStream != null) {
            outputStream.close();
            outputStream = null;
        }
    }
}
