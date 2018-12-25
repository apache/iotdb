package cn.edu.tsinghua.iotdb.queryV2.engine.control;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.utils.CommonUtils;

/**
 * Manager all file streams opened by overflow. Every overflow's read job has a unique ID which is saved in corresponding
 * SeriesReader
 * Created by zhangjinrui on 2018/1/18.
 */
public class OverflowFileStreamManager {
	private static final Logger LOGGER = LoggerFactory.getLogger(OverflowFileStreamManager.class);
    private ConcurrentHashMap<Long, Map<String, RandomAccessFile>> fileStreamStore;

    private ConcurrentHashMap<String, MappedByteBuffer> memoryStreamStore = new ConcurrentHashMap<>();

    private AtomicInteger mappedByteBufferUsage = new AtomicInteger();

    private OverflowFileStreamManager() {
        fileStreamStore = new ConcurrentHashMap<>();
    }

    // Using MMap to replace RandomAccessFile
    public synchronized MappedByteBuffer get(String path) throws IOException {
        if (!memoryStreamStore.containsKey(path)) {
            RandomAccessFile randomAccessFile = new RandomAccessFile(path, "r");
            MappedByteBuffer mappedByteBuffer = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, randomAccessFile.length());
            memoryStreamStore.put(path, mappedByteBuffer);
            mappedByteBufferUsage.set(mappedByteBufferUsage.get() + (int)randomAccessFile.length());
        }
        return memoryStreamStore.get(path);
    }

    public boolean contains(String path) {
        return memoryStreamStore.containsKey(path);
    }

    /**
     * Remove the MMap usage of given path.
     *
     * @param path
     */
    public synchronized void removeMappedByteBuffer(String path) {
        if (memoryStreamStore.containsKey(path)) {
            MappedByteBuffer buffer = memoryStreamStore.get(path);
            mappedByteBufferUsage.set(mappedByteBufferUsage.get() - buffer.limit());
            try {
				CommonUtils.destroyBuffer(buffer);
			} catch (Exception e) {
				LOGGER.error("Failed to remove MappedByteBuffer for {} because of {}.", path, e);
			}
            // Only support in JDK8
            // ((DirectBuffer) buffer).cleaner().clean();
        }
    }

    public AtomicInteger getMappedByteBufferUsage() {
        return this.mappedByteBufferUsage;
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