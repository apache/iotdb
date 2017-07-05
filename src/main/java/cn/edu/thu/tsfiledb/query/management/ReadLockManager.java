package cn.edu.thu.tsfiledb.query.management;

import java.util.HashMap;

import cn.edu.thu.tsfiledb.engine.exception.FileNodeManagerException;
import cn.edu.thu.tsfiledb.engine.filenode.FileNodeManager;
import cn.edu.thu.tsfiledb.exception.NotConsistentException;
import cn.edu.thu.tsfiledb.query.reader.RecordReader;
import cn.edu.thu.tsfile.common.exception.ProcessorException;


public class ReadLockManager {

    private static ReadLockManager instance = new ReadLockManager();
    private FileNodeManager fileNodeManager = FileNodeManager.getInstance();
    private ThreadLocal<HashMap<String, Integer>> locksMap = new ThreadLocal<>();
    public RecordReaderCache recordReaderCache = new RecordReaderCache();

    private ReadLockManager() {
    }

    public int lock(String deltaObjectUID, String measurementID) throws ProcessorException {
        checkLocksMap();
        String key = getKey(deltaObjectUID, measurementID);
        int token;
        if (!locksMap.get().containsKey(key)) {
            try {
                token = fileNodeManager.beginQuery(deltaObjectUID);
            } catch (FileNodeManagerException e) {
                e.printStackTrace();
                throw new ProcessorException(e.getMessage());
            }
            locksMap.get().put(key, token);
        } else {
            token = locksMap.get().get(key);
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

    public void unlockForOneRequest() throws NotConsistentException, ProcessorException {
        if (locksMap.get() == null) {
            throw new NotConsistentException("There is no locks in last request");
        }
        HashMap<String, Integer> locks = locksMap.get();
        for (String key : locks.keySet()) {
            unlockForQuery(key, locks.get(key));
        }
        locksMap.remove();
        //remove recordReaders cached
        recordReaderCache.clear();
    }

    public String getKey(String deltaObjectUID, String measurementID) {
        return deltaObjectUID;
    }

//	public String[] splitKey(String key){
//		return key.split("#");
//	}

    public static ReadLockManager getInstance() {
        if (instance == null) {
            instance = new ReadLockManager();
        }
        return instance;
    }

    private void checkLocksMap() {
        if (locksMap.get() == null) {
            locksMap.set(new HashMap<String, Integer>());
        }
    }
}
