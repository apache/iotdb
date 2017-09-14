package cn.edu.tsinghua.iotdb.query.management;

import java.util.HashMap;

import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;


public class ReadLockManager {

    private static ReadLockManager instance = new ReadLockManager();
    private FileNodeManager fileNodeManager = FileNodeManager.getInstance();
    // storage deltaObjectId and its read lock
    private ThreadLocal<HashMap<String, Integer>> locksMap = new ThreadLocal<>();
    public RecordReaderCache recordReaderCache = new RecordReaderCache();

    private ReadLockManager() {
    }

    public int lock(String deltaObjectUID, String measurementID) throws ProcessorException {
        checkLocksMap();
        int token;
        if (!locksMap.get().containsKey(deltaObjectUID)) {
            try {
                token = fileNodeManager.beginQuery(deltaObjectUID);
            } catch (FileNodeManagerException e) {
                e.printStackTrace();
                throw new ProcessorException(e.getMessage());
            }
            locksMap.get().put(deltaObjectUID, token);
        } else {
            token = locksMap.get().get(deltaObjectUID);
        }
        return token;
    }

    public void unlockForSubQuery(String deltaObjectUID, String measurementID
            , int token) throws ProcessorException {

    }

    private void unlockForQuery(String deltaObjectUID, int token) throws ProcessorException {
        try {
            fileNodeManager.endQuery(deltaObjectUID, token);
        } catch (FileNodeManagerException e) {
            e.printStackTrace();
            throw new ProcessorException(e.getMessage());
        }
    }

    /**
     * When jdbc connection is closed normally or quit abnormally, this method should be invoked.<br>
     * All read cache in this request should be released.
     *
     * @throws ProcessorException
     */
    public void unlockForOneRequest() throws ProcessorException {
        if (locksMap.get() == null) {
            return;
        }
        HashMap<String, Integer> locks = locksMap.get();
        for (String key : locks.keySet()) {
            unlockForQuery(key, locks.get(key));
        }
        locksMap.remove();
        //remove recordReaders cached
        recordReaderCache.clear();
    }

    public static ReadLockManager getInstance() {
        return instance;
    }

    private void checkLocksMap() {
        if (locksMap.get() == null) {
            locksMap.set(new HashMap<>());
        }
    }
}
