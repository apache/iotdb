package cn.edu.tsinghua.iotdb.query.management;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.engine.filenode.IntervalFileNode;
import cn.edu.tsinghua.iotdb.engine.filenode.QueryStructure;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.query.reader.RecordReader;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;

/**
 * To avoid create RecordReader frequently,<br>
 * RecordReaderFactory could create a RecordReader using cache.
 *
 * @author Jinrui Zhang
 */
public class RecordReaderFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordReaderFactory.class);
    private static RecordReaderFactory instance = new RecordReaderFactory();

    private FileNodeManager fileNodeManager;
    private ReadLockManager readLockManager;

    private RecordReaderFactory() {
        fileNodeManager = FileNodeManager.getInstance();
        readLockManager = ReadLockManager.getInstance();
    }

    /**
     * Construct a RecordReader which contains QueryStructure and read lock token.
     *
     * @param readLock if readLock is not null, the read lock of file node has been created,<br>
     *                 else a new read lock token should be applied.
     * @param prefix   for the exist of <code>RecordReaderCache</code> and batch read, we need a prefix to
     *                 represent the uniqueness.
     * @return <code>RecordReader</code>
     * @throws ProcessorException
     */
    public RecordReader getRecordReader(String deltaObjectUID, String measurementID,
                                        SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter,
                                        Integer readLock, String prefix) throws ProcessorException, PathErrorException {
        int token = 0;
        if (readLock == null) {
            token = readLockManager.lock(deltaObjectUID);
        } else {
            token = readLock;
        }
        String cacheDeltaKey = prefix + deltaObjectUID;
        if (readLockManager.recordReaderCache.containsRecordReader(cacheDeltaKey, measurementID)) {
            return readLockManager.recordReaderCache.get(cacheDeltaKey, measurementID);
        } else {
            QueryStructure queryStructure;
            try {
                queryStructure = fileNodeManager.query(deltaObjectUID, measurementID, timeFilter, freqFilter, valueFilter);
                // LOGGER.debug(queryStructure.toString());
            } catch (FileNodeManagerException e) {
                throw new ProcessorException(e.getMessage());
            }
            RecordReader recordReader = createANewRecordReader(deltaObjectUID, measurementID, queryStructure, token);
            readLockManager.recordReaderCache.put(cacheDeltaKey, measurementID, recordReader);
            return recordReader;
        }
    }

    private RecordReader createANewRecordReader(String deltaObjectUID, String measurementID,
                                                QueryStructure queryStructure, int token) throws ProcessorException, PathErrorException {
        RecordReader recordReader;

        List<IntervalFileNode> fileNodes = queryStructure.getBufferwriteDataInFiles();
        boolean hasUnEnvelopedFile;
        if (fileNodes.size() > 0 && !fileNodes.get(fileNodes.size() - 1).isClosed()) {
            hasUnEnvelopedFile = true;
        } else {
            hasUnEnvelopedFile = false;
        }
        List<String> filePathList = new ArrayList<>();
        for (int i = 0; i < fileNodes.size() - 1; i++) {
            IntervalFileNode fileNode = fileNodes.get(i);
            filePathList.add(fileNode.getFilePath());
        }
        if (hasUnEnvelopedFile) {
            String unsealedFilePath = fileNodes.get(fileNodes.size() - 1).getFilePath();

            // if currentPage is null, both currentPage and pageList must both are null
            if (queryStructure.getCurrentPage() == null) {
                recordReader = new RecordReader(filePathList, unsealedFilePath, queryStructure.getBufferwriteDataInDisk(),
                        deltaObjectUID, measurementID, token, null, null, null,
                        queryStructure.getAllOverflowData());
            } else {
                recordReader = new RecordReader(filePathList, unsealedFilePath, queryStructure.getBufferwriteDataInDisk(),
                        deltaObjectUID, measurementID, token, queryStructure.getCurrentPage(),
                        queryStructure.getPageList().left, queryStructure.getPageList().right, queryStructure.getAllOverflowData());
            }
        } else {
            if (fileNodes.size() > 0) {
                filePathList.add(fileNodes.get(fileNodes.size() - 1).getFilePath());
            }
            if (queryStructure.getCurrentPage() == null) {
                recordReader = new RecordReader(filePathList, deltaObjectUID, measurementID, token,
                        queryStructure.getCurrentPage(), null, null, queryStructure.getAllOverflowData());
            } else {
                recordReader = new RecordReader(filePathList, deltaObjectUID, measurementID, token,
                        queryStructure.getCurrentPage(), queryStructure.getPageList().left, queryStructure.getPageList().right,
                        queryStructure.getAllOverflowData());
            }
        }

        return recordReader;

    }

    public static RecordReaderFactory getInstance() {
        return instance;
    }

    // TODO this method is only used in test case and KV-match index
    public void removeRecordReader(String deltaObjectId, String measurementId) throws IOException, ProcessorException {
        if (readLockManager.recordReaderCache.containsRecordReader(deltaObjectId, measurementId)) {
            // close the RecordReader read stream.
            readLockManager.recordReaderCache.get(deltaObjectId, measurementId).closeFileStream();
            readLockManager.recordReaderCache.get(deltaObjectId, measurementId).clearReaderMaps();
            readLockManager.recordReaderCache.remove(deltaObjectId, measurementId);
        }
    }
}
