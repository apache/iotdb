package cn.edu.tsinghua.iotdb.query.reader;

import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FileReaderMap {
    /** map to store opened file stream **/
    private static ThreadLocal<Map<String, TsRandomAccessLocalFileReader>> fileReaderMap = new ThreadLocal<>();

    private static class ReaderHolder {
        private static final FileReaderMap INSTANCE = new FileReaderMap();
    }

    public static FileReaderMap getInstance() {
        return ReaderHolder.INSTANCE;
    }

    public TsRandomAccessLocalFileReader get(String path) throws IOException {
        if (fileReaderMap.get() == null) {
            fileReaderMap.set(new HashMap<>());
        }

        TsRandomAccessLocalFileReader fileReader;
        if (!fileReaderMap.get().containsKey(path)) {
            fileReader = new TsRandomAccessLocalFileReader(path);
            fileReaderMap.get().put(path, fileReader);
        } else {
            fileReader = fileReaderMap.get().get(path);
        }

        return fileReader;
    }

    public void close() throws IOException {
        if (fileReaderMap.get() != null) {
            for (Map.Entry<String, TsRandomAccessLocalFileReader> entry : fileReaderMap.get().entrySet()) {
                TsRandomAccessLocalFileReader reader = entry.getValue();
                reader.close();
            }

            fileReaderMap.get().clear();
            fileReaderMap.remove();
        }
    }
}
