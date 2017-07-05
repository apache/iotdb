package cn.edu.thu.tsfiledb.query.management;

import java.io.IOException;
import java.util.HashMap;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfiledb.query.reader.RecordReader;

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

    public void clear() throws ProcessorException {
        for (RecordReader rr : cache.get().values()) {
            try {
                rr.close();
            } catch (IOException | ProcessorException e) {
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
