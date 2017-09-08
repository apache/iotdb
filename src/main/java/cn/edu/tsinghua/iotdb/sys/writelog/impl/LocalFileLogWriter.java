package cn.edu.tsinghua.iotdb.sys.writelog.impl;

import java.io.*;

import cn.edu.tsinghua.iotdb.sys.writelog.WriteLogPersistable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author CGF
 */
public class LocalFileLogWriter implements WriteLogPersistable {
    private static final Logger LOG = LoggerFactory.getLogger(LocalFileLogWriter.class);
    private String logFile;
    private FileOutputStream os = null;

    public LocalFileLogWriter(String path) throws IOException {
        logFile = path;
        File f = new File(path);
        if (!f.getParentFile().exists()) {
            if(!f.getParentFile().mkdirs()) {
                LOG.error("System log directory create failed!");
                throw new IOException("System log directory create failed!");
            }
        }
        os = new FileOutputStream(path, true);
    }

    @Override
    public void write(byte[] operator) throws IOException {
        if (os == null) {
            os = new FileOutputStream(logFile);
        }
        os.write(operator);
        flush();
    }

    @Override
    public void close() {
        try {
            os.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void flush() throws IOException {
        os.flush();
    }

}
