package cn.edu.thu.tsfiledb.query.management;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileReader;
import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfiledb.engine.exception.FileNodeManagerException;
import cn.edu.thu.tsfiledb.engine.filenode.FileNodeManager;
import cn.edu.thu.tsfiledb.engine.filenode.IntervalFileNode;
import cn.edu.thu.tsfiledb.engine.filenode.QueryStructure;
import cn.edu.thu.tsfiledb.query.reader.RecordReader;

/**
 * To avoid create RecordReader frequently,<br>
 * RecordReaderFactory could create a RecordReader using cache.
 * 
 * @author Jinrui Zhang
 *
 */
public class RecordReaderFactory {
	private static final Logger LOGGER = LoggerFactory.getLogger(RecordReaderFactory.class);
	private static RecordReaderFactory instance = new RecordReaderFactory();

	private FileNodeManager fileNodeManager;
	private ReadLockManager readLockManager;
	private FileStreamManager fileStreamManager;

	private RecordReaderFactory() {
		fileNodeManager = FileNodeManager.getInstance();
		readLockManager = ReadLockManager.getInstance();
		fileStreamManager = FileStreamManager.getInstance();
	}

	public RecordReader getRecordReader(String deltaObjectUID, String measurementID,
			SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter)
			throws ProcessorException {
		int token = readLockManager.lock(deltaObjectUID, measurementID);
		if (readLockManager.recordReaderCache.containsRecordReader(deltaObjectUID, measurementID)) {
			return readLockManager.recordReaderCache.get(deltaObjectUID, measurementID);
		} else {
			QueryStructure queryStructure;
			try {
				queryStructure = fileNodeManager.query(deltaObjectUID, measurementID, timeFilter, freqFilter, valueFilter);
				LOGGER.info(queryStructure.toString());
			} catch (FileNodeManagerException e) {
				throw new ProcessorException(e.getMessage());
			}
			RecordReader recordReader = createANewRecordReader(deltaObjectUID, measurementID, queryStructure, token);
			readLockManager.recordReaderCache.put(deltaObjectUID, measurementID, recordReader);
			return recordReader;
		}
	}

	private RecordReader createANewRecordReader(String deltaObjectUID, String measurementID,
			QueryStructure queryStructure, int token) throws ProcessorException {
		RecordReader recordReader;

		List<IntervalFileNode> fileNodes = queryStructure.getBufferwriteDataInFiles();
		boolean hasUnEnvelopedFile;
		if (fileNodes.size() > 0 && !fileNodes.get(fileNodes.size() - 1).isClosed()) {
			hasUnEnvelopedFile = true;
		} else {
			hasUnEnvelopedFile = false;
		}
		List<TSRandomAccessFileReader> rafList = new ArrayList<>();
		try {
			for (int i = 0; i < fileNodes.size() - 1; i++) {
				IntervalFileNode fileNode = fileNodes.get(i);
				TSRandomAccessFileReader raf = fileStreamManager.getLocalRandomAccessFileReader(fileNode.filePath);
				rafList.add(raf);
			}
			if (hasUnEnvelopedFile) {
				TSRandomAccessFileReader unsealedRaf = fileStreamManager
						.getLocalRandomAccessFileReader(fileNodes.get(fileNodes.size() - 1).filePath);

				// if currentPage is null, both currentPage and pageList must both are null
				if (queryStructure.getCurrentPage() == null) {
					recordReader = new RecordReader(rafList, unsealedRaf, queryStructure.getBufferwriteDataInDisk(),
							deltaObjectUID, measurementID, token, null, null, null,
							queryStructure.getAllOverflowData());
				} else {
					recordReader = new RecordReader(rafList, unsealedRaf, queryStructure.getBufferwriteDataInDisk(),
							deltaObjectUID, measurementID, token, queryStructure.getCurrentPage(),
							queryStructure.getPageList().left, queryStructure.getPageList().right, queryStructure.getAllOverflowData());
				}
			} else {
				if (fileNodes.size() > 0) {
					rafList.add(fileStreamManager
							.getLocalRandomAccessFileReader(fileNodes.get(fileNodes.size() - 1).filePath));
				}
				if (queryStructure.getCurrentPage() == null) {
					recordReader = new RecordReader(rafList, deltaObjectUID, measurementID, token,
							queryStructure.getCurrentPage(), null, null, queryStructure.getAllOverflowData());
				} else {
					recordReader = new RecordReader(rafList, deltaObjectUID, measurementID, token,
							queryStructure.getCurrentPage(), queryStructure.getPageList().left, queryStructure.getPageList().right,
							queryStructure.getAllOverflowData());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new ProcessorException(e.getMessage());
		}
		return recordReader;

	}

	//TODO this just close the RecordReader but never remove it from cache?
	public void closeOneRecordReader(RecordReader recordReader) throws ProcessorException {
//		try {
//			recordReader.close();
//		} catch (IOException e) {
//			logger.error("Error in closing RecordReader : {}", e.getMessage());
//			e.printStackTrace();
//		}
	}

	public static RecordReaderFactory getInstance() {
		return instance;
	}

	public void removeRecordReader(String deltaObjectId, String measurementId) {
		if (readLockManager.recordReaderCache.containsRecordReader(deltaObjectId, measurementId)) {
			readLockManager.recordReaderCache.remove(deltaObjectId, measurementId);
		}
	}
}
