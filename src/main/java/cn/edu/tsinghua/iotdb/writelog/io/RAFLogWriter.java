package cn.edu.tsinghua.iotdb.writelog.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.zip.CRC32;

public class RAFLogWriter implements ILogWriter {

    private File logFile;
    private RandomAccessFile raf;
    private CRC32 checkSummer = new CRC32();

    public RAFLogWriter(String logFilePath) {
        logFile = new File(logFilePath);
    }

    @Override
    public void write(List<byte[]> logCache) throws IOException {
        if (raf == null)
            raf = new RandomAccessFile(logFile, "rw");
        raf.seek(raf.length());
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
        raf.write(buffer.array());
    }

    @Override
    public void close() throws IOException {
        if (raf != null) {
            raf.close();
            raf = null;
        }
    }
}
