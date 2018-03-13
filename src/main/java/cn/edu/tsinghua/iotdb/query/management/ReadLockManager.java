package cn.edu.tsinghua.iotdb.query.management;

import java.io.IOException;
import java.util.HashMap;

import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.query.engine.groupby.GroupByEngineNoFilter;
import cn.edu.tsinghua.iotdb.query.engine.groupby.GroupByEngineWithFilter;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;

/**
 * <p>
 * Read process lock manager and ThreadLocal variable manager.
 * When a query process is over or quit abnormally, the <code>unlockForOneRequest</code> method will
 * be invoked to clear the thread level variable.
 * </p>
 *
 */
public class ReadLockManager {

    private static ReadLockManager instance = new ReadLockManager();

    private FileNodeManager fileNodeManager = FileNodeManager.getInstance();

    /** storage deltaObjectId and its read lock **/
    private ThreadLocal<HashMap<String, Integer>> locksMap = new ThreadLocal<>();

    /** this is no need to set as ThreadLocal, RecordReaderCache has ThreadLocal variable**/
    public RecordReaderCache recordReaderCache = new RecordReaderCache();

    /** represents the execute time of group by method**/
    private ThreadLocal<Integer> groupByCalcTime;

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

    /**
     * When jdbc connection is closed normally or quit abnormally, this method should be invoked.<br>
     * All read cache in this request should be released.
     *
     * @throws ProcessorException
     */
    public void unlockForOneRequest() throws ProcessorException, IOException {
        if (locksMap.get() == null) {
            return;
        }
        HashMap<String, Integer> locks = locksMap.get();
        for (String key : locks.keySet()) {
            unlockForQuery(key, locks.get(key));
        }
        locksMap.remove();


        if (groupByCalcTime != null && groupByCalcTime.get() != null) {
            groupByCalcTime.remove();
        }
        if (groupByEngineNoFilterLocal != null && groupByEngineNoFilterLocal.get() != null) {
            groupByEngineNoFilterLocal.remove();
        }
        if (groupByEngineWithFilterLocal != null && groupByEngineWithFilterLocal.get() != null) {
            groupByEngineWithFilterLocal.remove();
        }

        recordReaderCache.clear();
        FileReaderMap.getInstance().close();
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

    public ThreadLocal<Integer> getGroupByCalcTime() {
        if (groupByCalcTime == null) {
            groupByCalcTime = new ThreadLocal<>();
        }
        return this.groupByCalcTime;
    }

    public void setGroupByCalcTime(ThreadLocal<Integer> t) {
        this.groupByCalcTime = t;
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
