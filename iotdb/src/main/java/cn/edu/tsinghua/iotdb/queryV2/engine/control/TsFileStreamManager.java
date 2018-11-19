package cn.edu.tsinghua.iotdb.queryV2.engine.control;

import cn.edu.tsinghua.iotdb.queryV2.engine.component.job.QueryJob;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhangjinrui on 2018/1/14.
 */
public class TsFileStreamManager {

    private ConcurrentHashMap<QueryJob, TsFileReaderCacheForOneQuery> cache;

    private TsFileStreamManager() {
        cache = new ConcurrentHashMap<>();
    }

    /**
     * Get or open a FileStream for one query
     * @param queryJob
     * @param path
     * @return
     * @throws FileNotFoundException
     */
    public ITsRandomAccessFileReader getTsFileStreamReader(QueryJob queryJob, String path) throws FileNotFoundException {
        if (!cache.containsKey(queryJob)) {
            cache.put(queryJob, new TsFileReaderCacheForOneQuery());
        }
        return cache.get(queryJob).get(path);
    }

    /**
     * release one FileStream for one query
     * @param queryJob
     * @param path
     * @throws IOException
     */
    public void release(QueryJob queryJob, String path) throws IOException {
        cache.get(queryJob).release(path);
    }

    /**
     * Close all opened FileStreams in one query
     * @param queryJob
     * @throws IOException
     */
    public void releaseAll(QueryJob queryJob) throws IOException {
        cache.get(queryJob).close();
        cache.remove(queryJob);
    }

    private class TsFileReaderCacheForOneQuery {
        private Map<String, CachedTsFileReader> cache;

        private TsFileReaderCacheForOneQuery() {
            this.cache = new HashMap<>();
        }

        public ITsRandomAccessFileReader get(String path) throws FileNotFoundException {
            if (!cache.containsKey(path)) {
                cache.put(path, new CachedTsFileReader(new TsRandomAccessLocalFileReader(path), path));
            }
            CachedTsFileReader cachedTsFileReader = cache.get(path);
            cachedTsFileReader.increaseReferenceCount();
            return cachedTsFileReader.getTsRandomAccessFileReader();
        }

        public void release(String path) throws IOException {
            if (cache.containsKey(path)) {
                CachedTsFileReader cachedTsFileReader = cache.get(path);
                cachedTsFileReader.minusReferenceCount();
                if (cachedTsFileReader.getReferenceCount() == 0) {
                    cachedTsFileReader.close();
                    cache.remove(path);
                }
            }
        }

        public void close() throws IOException {
            for (CachedTsFileReader cachedTsFileReader : cache.values()) {
                if (cachedTsFileReader.getReferenceCount() > 0) {
                    cachedTsFileReader.close();
                }
            }
        }
    }

    private static class FileStreamManagerHelper {
        private static TsFileStreamManager INSTANCE = new TsFileStreamManager();
    }

    public static TsFileStreamManager getInstance() {
        return FileStreamManagerHelper.INSTANCE;
    }

    private class CachedTsFileReader {
        private ITsRandomAccessFileReader tsRandomAccessFileReader;
        private String path;
        private int referenceCount;

        private CachedTsFileReader(ITsRandomAccessFileReader tsRandomAccessFileReader, String path) {
            this.tsRandomAccessFileReader = tsRandomAccessFileReader;
            this.path = path;
            this.referenceCount = 0;
        }

        public void increaseReferenceCount() {
            this.referenceCount++;
        }

        public void minusReferenceCount() {
            this.referenceCount--;
        }

        public ITsRandomAccessFileReader getTsRandomAccessFileReader() {
            return tsRandomAccessFileReader;
        }

        public int getReferenceCount() {
            return referenceCount;
        }

        public String getPath() {
            return path;
        }

        public void close() throws IOException {
            this.tsRandomAccessFileReader.close();
        }
    }
}
