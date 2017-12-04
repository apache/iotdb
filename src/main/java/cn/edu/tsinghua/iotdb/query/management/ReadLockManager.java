package cn.edu.tsinghua.iotdb.query.management;

import java.util.HashMap;

import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.query.engine.groupby.GroupByEngineNoFilter;
import cn.edu.tsinghua.iotdb.query.engine.groupby.GroupByEngineWithFilter;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;


public class ReadLockManager {

    private static ReadLockManager instance = new ReadLockManager();

    private FileNodeManager fileNodeManager = FileNodeManager.getInstance();

    /** storage deltaObjectId and its read lock **/
    private ThreadLocal<HashMap<String, Integer>> locksMap = new ThreadLocal<>();

    /** this is no need to set as ThreadLocal, RecordReaderCache has ThreadLocal variable**/
    public RecordReaderCache recordReaderCache = new RecordReaderCache();

    /** if this variable equals true, represent that the group by method is executed the first time**/
    private ThreadLocal<Integer> groupByCalcCalcTime;

    /** ThreadLocal, due to the usage of OverflowQPExecutor **/
    private ThreadLocal<GroupByEngineNoFilter> groupByEngineNoFilterLocal;

    /** ThreadLocal, due to the usage of OverflowQPExecutor **/
    private ThreadLocal<GroupByEngineWithFilter> groupByEngineWithFilterLocal;

    private ReadLockManager() {
    }

    public int lock(String deltaObjectUID) throws ProcessorException {
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

    @Deprecated
    public void unlockForSubQuery(String deltaObjectUID, String measurementID
            , int token) throws ProcessorException {

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
        recordReaderCache.clear();

        if (groupByCalcCalcTime != null && groupByCalcCalcTime.get() != null) {
            groupByCalcCalcTime.remove();
        }
        if (groupByEngineNoFilterLocal != null && groupByEngineNoFilterLocal.get() != null) {
            groupByEngineNoFilterLocal.remove();
        }
        if (groupByEngineWithFilterLocal != null && groupByEngineWithFilterLocal.get() != null) {
            groupByEngineWithFilterLocal.remove();
        }
    }

    private void unlockForQuery(String deltaObjectUID, int token) throws ProcessorException {
        try {
            fileNodeManager.endQuery(deltaObjectUID, token);
        } catch (FileNodeManagerException e) {
            e.printStackTrace();
            throw new ProcessorException(e.getMessage());
        }
    }

    public static ReadLockManager getInstance() {
        return instance;
    }

    private void checkLocksMap() {
        if (locksMap.get() == null) {
            locksMap.set(new HashMap<>());
        }
    }

    public ThreadLocal<Integer> getGroupByCalcCalcTime() {
        if (groupByCalcCalcTime == null) {
            groupByCalcCalcTime = new ThreadLocal<>();
        }
        return this.groupByCalcCalcTime;
    }

    public void setGroupByCalcCalcTime(ThreadLocal<Integer> t) {
        this.groupByCalcCalcTime = t;
    }

    public ThreadLocal<GroupByEngineNoFilter> getGroupByEngineNoFilterLocal() {
        if (groupByEngineNoFilterLocal == null) {
            groupByEngineNoFilterLocal = new ThreadLocal<>();
        }
        return this.groupByEngineNoFilterLocal;
    }

    public void setGroupByEngineNoFilterLocal(ThreadLocal<GroupByEngineNoFilter> t) {
        this.groupByEngineNoFilterLocal = t;
    }

    public ThreadLocal<GroupByEngineWithFilter> getGroupByEngineWithFilterLocal() {
        if (groupByEngineWithFilterLocal == null) {
            groupByEngineWithFilterLocal = new ThreadLocal<>();
        }
        return this.groupByEngineWithFilterLocal;
    }

    public void setGroupByEngineWithFilterLocal(ThreadLocal<GroupByEngineWithFilter> t) {
        this.groupByEngineWithFilterLocal = t;
    }
}
