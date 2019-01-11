package cn.edu.tsinghua.iotdb.query.control;

import cn.edu.tsinghua.iotdb.concurrent.IoTDBThreadPoolFactory;
import cn.edu.tsinghua.iotdb.conf.IoTDBDescriptor;
import cn.edu.tsinghua.iotdb.exception.StartupException;
import cn.edu.tsinghua.iotdb.service.IService;
import cn.edu.tsinghua.iotdb.service.ServiceType;
import cn.edu.tsinghua.tsfile.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.read.UnClosedTsFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static cn.edu.tsinghua.iotdb.service.ServiceType.FILE_READER_MANAGER_SERVICE;

/**
 * <p> Singleton pattern, to manage all file reader.
 * Manage all opened file streams, to ensure that each file will be opened at most once.
 */
public class FileReaderManager implements IService {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileReaderManager.class);

    /**
     * max file stream storage number, must be lower than 65535
     */
    private static final int MAX_CACHED_FILE_SIZE = 30000;

    /**
     * key of fileReaderMap file path, value of fileReaderMap is its unique reader.
     */
    private ConcurrentHashMap<String, TsFileSequenceReader> fileReaderMap;

    /**
     * key of fileReaderMap file path, value of fileReaderMap is its reference count.
     */
    private ConcurrentHashMap<String, AtomicInteger> referenceMap;

    private ScheduledExecutorService executorService;

    private FileReaderManager() {
        fileReaderMap = new ConcurrentHashMap<>();
        referenceMap = new ConcurrentHashMap<>();
        executorService = IoTDBThreadPoolFactory.newScheduledThreadPool(1, "opended-files-manager");

        clearUnUsedFilesInFixTime();
    }

    private void clearUnUsedFilesInFixTime() {

        long examinePeriod = IoTDBDescriptor.getInstance().getConfig().cacheFileReaderClearPeriod;

        executorService.scheduleAtFixedRate(() -> {
            synchronized (this) {
                for (Map.Entry<String, TsFileSequenceReader> entry : fileReaderMap.entrySet()) {
                    TsFileSequenceReader reader = entry.getValue();
                    int referenceNum = referenceMap.get(entry.getKey()).get();

                    if (referenceNum == 0) {
                        try {
                            reader.close();
                        } catch (IOException e) {
                            LOGGER.error("Can not close TsFileSequenceReader {} !", reader.getFileName());
                        }
                        fileReaderMap.remove(entry.getKey());
                        referenceMap.remove(entry.getKey());
                    }
                }
            }
        },0, examinePeriod, TimeUnit.MILLISECONDS);
    }

    /**
     * Given a file path, tsfile or unseq tsfile, return a <code>TsFileSequenceReader</code> which
     * opened this file.
     */
    public synchronized TsFileSequenceReader get(String filePath, boolean isUnClosed) throws IOException {

        if (!fileReaderMap.containsKey(filePath)) {

            if (fileReaderMap.size() >= MAX_CACHED_FILE_SIZE) {
                LOGGER.warn("Query has opened {} files !", fileReaderMap.size());
            }

            TsFileSequenceReader tsFileReader = isUnClosed ? new UnClosedTsFileReader(filePath) : new TsFileSequenceReader(filePath);

            fileReaderMap.put(filePath, tsFileReader);
            return tsFileReader;
        }

        return fileReaderMap.get(filePath);
    }

    /**
     * Increase the usage reference of given file path.
     * Only when the reference of given file path equals to zero, the corresponding file reader can be closed and remove.
     */
    public synchronized void increaseFileReaderReference(String filePath) {
        referenceMap.computeIfAbsent(filePath, k -> new AtomicInteger()).getAndIncrement();
    }

    /**
     * Decrease the usage reference of given file path.
     * This method doesn't need lock.
     * Only when the reference of given file path equals to zero, the corresponding file reader can be closed and remove.
     */
    public synchronized void decreaseFileReaderReference(String filePath) {
        referenceMap.get(filePath).getAndDecrement();
    }

    /**
     * This method is used when the given file path is deleted.
     */
    public synchronized void closeFileAndRemoveReader(String filePath) throws IOException {
        System.out.println(fileReaderMap.containsKey(filePath));
        if (fileReaderMap.containsKey(filePath)) {
            referenceMap.remove(filePath);
            fileReaderMap.get(filePath).close();
            fileReaderMap.remove(filePath);
        }
    }

    /**
     * Only used for <code>EnvironmentUtils.cleanEnv</code> method.
     * To make sure that unit test and integration test will not make conflict.
     */
    public synchronized void closeAndRemoveAllOpenedReaders() throws IOException {
        for (Map.Entry<String, TsFileSequenceReader> entry : fileReaderMap.entrySet()) {
            entry.getValue().close();
            referenceMap.remove(entry.getKey());
            fileReaderMap.remove(entry.getKey());
        }
    }

    /**
     * This method is only used for unit test
     */
    public synchronized boolean contains(String filePath) {
        return fileReaderMap.containsKey(filePath);
    }

    @Override
    public void start() throws StartupException {
    }

    @Override
    public void stop() {
        if (executorService == null || executorService.isShutdown()) {
            return;
        }

        executorService.shutdown();
        try {
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("StatMonitor timing service could not be shutdown.", e);
        }
    }

    @Override
    public ServiceType getID() {
        return FILE_READER_MANAGER_SERVICE;
    }

    private static class FileReaderManagerHelper {
        public static FileReaderManager INSTANCE = new FileReaderManager();
    }

    public static FileReaderManager getInstance() {
        return FileReaderManagerHelper.INSTANCE;
    }
}