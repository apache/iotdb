package cn.edu.tsinghua.iotdb.queryV2.engine.control;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manager all file streams opened by overflow. Every overflow's read job has a unique ID which is saved in corresponding
 * SeriesReader
 * Created by zhangjinrui on 2018/1/18.
 */
public class OverflowFileStreamManager {

    private ConcurrentHashMap<Long, Map<String, RandomAccessFile>> fileStreamStore;

    private OverflowFileStreamManager() {
        fileStreamStore = new ConcurrentHashMap<>();
    }

    public RandomAccessFile get(Long jobId, String path) throws IOException {
        if (!fileStreamStore.containsKey(jobId)) {
            fileStreamStore.put(jobId, new HashMap<>());
        }
        if (!fileStreamStore.get(jobId).containsKey(path)) {
            fileStreamStore.get(jobId).put(path, new RandomAccessFile(path, "r"));
        }
        return fileStreamStore.get(jobId).get(path);
    }

    public void closeAll(Long jobId) throws IOException {
        if (fileStreamStore.containsKey(jobId)) {
            for (RandomAccessFile randomAccessFile : fileStreamStore.get(jobId).values()) {
                randomAccessFile.close();
            }
            fileStreamStore.remove(jobId);
        }
    }

    private static class OverflowFileStreamManagerHelper {
        public static OverflowFileStreamManager INSTANCE = new OverflowFileStreamManager();
    }

    public static OverflowFileStreamManager getInstance() {
        return OverflowFileStreamManagerHelper.INSTANCE;
    }
}
