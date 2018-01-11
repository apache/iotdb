package cn.edu.tsinghua.iotdb.query.management;

import java.io.IOException;
import java.util.HashMap;

import cn.edu.tsinghua.iotdb.query.reader.RecordReader;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;

/**
 * Used for read process, put the query structure in the cache for one query process.
 */
public class RecordReaderCache {

    private ThreadLocal<HashMap<String, RecordReader>> cache = new ThreadLocal<>();

    public boolean containsRecordReader(String deltaObjectUID, String measurementID) {
        checkCacheInitialized();
        return cache.get().containsKey(getKey(deltaObjectUID, measurementID));
    }

    public RecordReader get(String deltaObjectUID, String measurementID) {
        checkCacheInitialized();
        return cache.get().get(getKey(deltaObjectUID, measurementID));
    }

    public void put(String deltaObjectUID, String measurementID, RecordReader recordReader) {
        checkCacheInitialized();
        cache.get().put(getKey(deltaObjectUID, measurementID), recordReader);
    }

    public RecordReader remove(String deltaObjectUID, String measurementID) {
        checkCacheInitialized();
        return cache.get().remove(getKey(deltaObjectUID, measurementID));
    }

    public void clear() throws ProcessorException {
        for (RecordReader reader : cache.get().values()) {
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
                throw new ProcessorException(e);
            }
        }
        cache.remove();
    }

    private String getKey(String deltaObjectUID, String measurementID) {
        return deltaObjectUID + "#" + measurementID;
    }

    private void checkCacheInitialized() {
        if (cache.get() == null) {
            cache.set(new HashMap<>());
        }
    }
}
