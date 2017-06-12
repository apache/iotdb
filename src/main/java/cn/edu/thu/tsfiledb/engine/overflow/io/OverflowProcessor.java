package cn.edu.thu.tsfiledb.engine.overflow.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.conf.TSFileConfig;
import cn.edu.thu.tsfile.common.conf.TSFileDescriptor;
import cn.edu.thu.tsfile.common.utils.BytesUtils;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfiledb.conf.TsfileDBConfig;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.engine.bufferwrite.Action;
import cn.edu.thu.tsfiledb.engine.bufferwrite.FileNodeConstants;
import cn.edu.thu.tsfiledb.engine.exception.OverflowProcessorException;
import cn.edu.thu.tsfiledb.engine.lru.LRUProcessor;
import cn.edu.thu.tsfiledb.engine.overflow.metadata.OFFileMetadata;
import cn.edu.thu.tsfiledb.engine.overflow.utils.ReadWriteThriftFormatUtils;
import cn.edu.thu.tsfiledb.engine.overflow.utils.TSFileMetaDataConverter;
import cn.edu.thu.tsfiledb.engine.overflow.utils.TimePair;
import cn.edu.thu.tsfiledb.engine.utils.FlushState;
import cn.edu.thu.tsfiledb.sys.writeLog.WriteLogManager;
import cn.edu.thu.tsfile.common.exception.ProcessorException;

public class OverflowProcessor extends LRUProcessor {

	private static final Logger LOGGER = LoggerFactory.getLogger(OverflowProcessor.class);
	private static final TSFileConfig TsFileConf = TSFileDescriptor.getInstance().getConfig();
	private static final TsfileDBConfig TsFileDBConf = TsfileDBDescriptor.getInstance().getConfig();

	private static final int MINIMUM_RECORD_COUNT_FOR_CHECK = 100;
	private static final int MAXIMUM_RECORD_COUNT_FOR_CHECK = 10000;
	private long recordCount = 0;
	private long recordCountForNextMemCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;

	private OverflowSupport ofSupport;
	private final int memoryBlockSize = TSFileDescriptor.getInstance().getConfig().rowGroupSize;

	private boolean isMerging = false;
	private final FlushState flushState = new FlushState();

	private static final String storeFileName = ".overflow";
	private static final String restoreFileName = ".restore";
	private static final String mergeFileName = ".merge";
	private String fileName;
	private String overflowRetoreFilePath;
	private String overflowOutputFilePath;
	private Action overflowFlushAction = null;
	private Action filenodeFlushAction = null;
	private Action filenodeManagerBackUpAction = null;
	private Action filenodeManagerFlushAction = null;

	public OverflowProcessor(String nameSpacePath, Map<String, Object> parameters) throws OverflowProcessorException {
		super(nameSpacePath);
		String overflowDirPath = TsFileDBConf.overflowDataDir;
		if (overflowDirPath.length() > 0
				&& overflowDirPath.charAt(overflowDirPath.length() - 1) != File.separatorChar) {
			overflowDirPath = overflowDirPath + File.separatorChar;
		}
		// overflow data dir
		String dataPath = overflowDirPath + nameSpacePath;
		File dataDir = new File(dataPath);
		if (!dataDir.exists()) {
			dataDir.mkdirs();
			LOGGER.warn("The overflow processor data dir doesn't exists, and mkdir the dir {}",
					dataDir.getAbsolutePath());
		}
		// overflow file name in the overflow data dir with the special
		// nameSpacePath.overflow
		fileName = nameSpacePath + storeFileName;
		overflowOutputFilePath = new File(dataDir, fileName).getAbsolutePath();
		overflowRetoreFilePath = overflowOutputFilePath + restoreFileName;

		// read information from overflow restore file
		OverflowStoreStruct overflowStoreStruct = readStoreFromDisk();
		long lastOverflowFilePostion = overflowStoreStruct.lastOverflowFilePosition;
		long lastOverflowRowGroupPosition = overflowStoreStruct.lastOverflowRowGroupPosition;
		OFFileMetadata ofFileMetadata = null;
		LOGGER.info("The overflow processor lastOverflowFilePostion is {}, lastOverflowRowGroupPosition is {}",
				lastOverflowFilePostion, lastOverflowRowGroupPosition);

		OverflowReadWriter raf;
		try {
			raf = new OverflowReadWriter(overflowOutputFilePath);
		} catch (IOException e) {
			LOGGER.error("Can't get the overflowReadWrite, the nameSpacePath is {}", nameSpacePath);
			throw new OverflowProcessorException("Can't get the overflowReadWrite, the reason is " + e.getMessage());
		}
		long lastUpdateOffset = 0;
		if (lastOverflowFilePostion == -1) {
			LOGGER.warn(
					"The overflow processor will recovery from rowgroup medata, the lastOverflowRowGroupPosition is {}",
					lastOverflowRowGroupPosition);
			lastUpdateOffset = lastOverflowRowGroupPosition;
			ofFileMetadata = overflowStoreStruct.ofFileMetadata;
		} else {
			LOGGER.info("The overflow processor will recovery from file medata, the lastOverflowFilePostion is {}",
					lastOverflowFilePostion);
			lastUpdateOffset = lastOverflowFilePostion;
			ofFileMetadata = null;
		}
		// create overflow file io
		OverflowFileIO overflowFileIO;
		try {
			overflowFileIO = new OverflowFileIO(raf, overflowOutputFilePath, lastUpdateOffset);
		} catch (IOException e) {
			LOGGER.error("Can't get the overflowFileIO, the nameSpacePath is {}", nameSpacePath);
			throw new OverflowProcessorException("Can't get the overflowFileIO, reason is " + e.getMessage());
		}
		// create overflow supoort
		try {
			this.ofSupport = new OverflowSupport(overflowFileIO, ofFileMetadata);
		} catch (IOException e) {
			LOGGER.error("Can't get the overflowSupport, the nameSpacePath is {}", nameSpacePath);
			throw new OverflowProcessorException("Can't get the overflowSupport, reason is " + e.getMessage());
		}

		overflowFlushAction = (Action) parameters.get(FileNodeConstants.OVERFLOW_FLUSH_ACTION);
		filenodeFlushAction = (Action) parameters.get(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION);
		filenodeManagerBackUpAction = (Action) parameters.get(FileNodeConstants.OVERFLOW_BACKUP_MANAGER_ACTION);
		filenodeManagerFlushAction = (Action) parameters.get(FileNodeConstants.OVERFLOW_FLUSH_MANAGER_ACTION);
	}

	/**
	 * This is used to store information about overflow file.<br>
	 * 
	 * @param lastOverflowFilePostion
	 *            -1 represent flush overflow row group - other represent close
	 *            overflow file
	 * @throws OverflowProcessorException
	 */
	private void writeStoreToDisk(long lastOverflowFilePostion, boolean isClose) throws OverflowProcessorException {
		synchronized (overflowRetoreFilePath) {

			FileOutputStream fileOutputStream = null;
			OFFileMetadata fileMetadata = new OFFileMetadata();
			long lastOverflowRowGroupPostion = -1;
			try {
				// the stream is closed, and can't get the position and metadata
				fileOutputStream = new FileOutputStream(overflowRetoreFilePath);
				fileOutputStream.write(BytesUtils.longToBytes(lastOverflowFilePostion));
				if (!isClose) {
					fileMetadata = ofSupport.getOFFileMetadata();
					lastOverflowRowGroupPostion = ofSupport.getPos();
					fileOutputStream.write(BytesUtils.longToBytes(lastOverflowRowGroupPostion));
					TSFileMetaDataConverter metadataConverter = new TSFileMetaDataConverter();
					ReadWriteThriftFormatUtils.writeOFFileMetaData(
							metadataConverter.toThriftOFFileMetadata(0, fileMetadata), fileOutputStream);

				} else {
					fileOutputStream.write(BytesUtils.longToBytes(lastOverflowRowGroupPostion));
				}

			} catch (IOException e) {
				LOGGER.error("Flush the information for the overflow processor error, the nameSpacePath is {}",
						nameSpacePath);
				e.printStackTrace();
				throw new OverflowProcessorException(
						"Flush the information for the overflow processor error, the nameSpacePath is " + nameSpacePath
								+ " the reason is " + e.getMessage());
			} finally {
				if (fileOutputStream != null) {
					try {
						fileOutputStream.close();
					} catch (IOException e) {
						e.printStackTrace();
						throw new OverflowProcessorException(e);
					}
				}
			}
		}
	}

	/**
	 * @return OverflowStoreStruct
	 * @throws OverflowProcessorException
	 */
	private OverflowStoreStruct readStoreFromDisk() throws OverflowProcessorException {
		synchronized (overflowRetoreFilePath) {

			File overflowRestoreFile = new File(overflowRetoreFilePath);
			if (!overflowRestoreFile.exists()) {
				return new OverflowStoreStruct(0, -1, null);
			}
			byte[] buff = new byte[8];
			FileInputStream fileInputStream = null;
			try {
				fileInputStream = new FileInputStream(overflowRestoreFile);
			} catch (FileNotFoundException e) {
				LOGGER.error("The overflow restore file is not found, the file path is {}", overflowRetoreFilePath);
				e.printStackTrace();
				throw new OverflowProcessorException(e);
			}
			int off = 0;
			int len = buff.length - off;
			cn.edu.thu.tsfiledb.engine.overflow.thrift.OFFileMetadata thriftfileMetadata = null;
			try {
				do {
					int num = fileInputStream.read(buff, off, len);
					off = off + num;
					len = len - num;
				} while (len > 0);
				long lastOverflowFilePosition = BytesUtils.bytesToLong(buff);

				if (lastOverflowFilePosition != -1) {
					return new OverflowStoreStruct(lastOverflowFilePosition, -1, null);
				}

				off = 0;
				len = buff.length - off;
				do {
					int num = fileInputStream.read(buff, off, len);
					off = off + num;
					len = len - num;
				} while (len > 0);

				long lastOverflowRowGroupPosition = BytesUtils.bytesToLong(buff);
				thriftfileMetadata = ReadWriteThriftFormatUtils.readOFFileMetaData(fileInputStream);
				TSFileMetaDataConverter metadataConverter = new TSFileMetaDataConverter();
				OFFileMetadata ofFileMetadata = metadataConverter.toOFFileMetadata(thriftfileMetadata);
				return new OverflowStoreStruct(lastOverflowFilePosition, lastOverflowRowGroupPosition, ofFileMetadata);
			} catch (IOException e) {
				LOGGER.error(
						"Read the data: lastOverflowFilePostion, lastOverflowRowGroupPostion, offilemetadata error");
				throw new OverflowProcessorException(
						"Read the data: lastOverflowFilePostion, lastOverflowRowGroupPostion, offilemetadata error");
			} finally {
				if (fileInputStream != null) {
					try {
						fileInputStream.close();
					} catch (IOException e) {
						e.printStackTrace();
						throw new OverflowProcessorException(e);
					}
				}
			}
		}

	}

	/**
	 * insert a list of data value in form of TimePair.
	 *
	 * @param deltaObjectId
	 *            - deltaObjectId to be insert
	 * @param measurementId
	 *            - measurementId to be insert
	 * @param dataPoints
	 *            - data points to be insert
	 * @throws ProcessorException
	 */
	public void insert(String deltaObjectId, String measurementId, TSDataType type, List<TimePair> dataPoints)
			throws OverflowProcessorException {
		for (TimePair timePair : dataPoints) {
			insert(deltaObjectId, measurementId, timePair.s, type, timePair.v);
		}
	}

	/**
	 * insert a point of data value
	 * 
	 * @param deltaObjectId
	 * @param measurementId
	 * @param timestamp
	 * @param type
	 * @param v
	 * @throws OverflowProcessorException
	 */
	public void insert(String deltaObjectId, String measurementId, long timestamp, TSDataType type, String v)
			throws OverflowProcessorException {
		insert(deltaObjectId, measurementId, timestamp, type, convertStringToBytes(type, v));
	}

	private void insert(String deltaObjectId, String measurementId, long timestamp, TSDataType type, byte[] v)
			throws OverflowProcessorException {
		if (ofSupport.insert(deltaObjectId, measurementId, timestamp, type, v)) {
			++recordCount;
			checkMemorySize();
		} else {
			LOGGER.error("The insert overflow record data type {} is not consistent with the data type in the metadata",
					type);
			throw new OverflowProcessorException(
					"The insert overflow record data type is error, insert type is " + type);
		}
	}

	/**
	 * update a point of data value
	 * 
	 * @param deltaObjectId
	 * @param measurementId
	 * @param timestamp
	 * @param type
	 * @param v
	 * @throws OverflowProcessorException
	 */
	public void update(String deltaObjectId, String measurementId, long timestamp, TSDataType type, String v)
			throws OverflowProcessorException {
		update(deltaObjectId, measurementId, timestamp, timestamp, type, v);
	}

	/**
	 * update a range of data value
	 * 
	 * @param deltaObjectId
	 * @param measurementId
	 * @param startTime
	 * @param endTime
	 * @param type
	 * @param v
	 * @throws OverflowProcessorException
	 */
	public void update(String deltaObjectId, String measurementId, long startTime, long endTime, TSDataType type,
			String v) throws OverflowProcessorException {
		if (ofSupport.update(deltaObjectId, measurementId, startTime, endTime, type, convertStringToBytes(type, v))) {
			++recordCount;
			checkMemorySize();
		} else {
			LOGGER.error("The update overflow record data type {} is not consistent with the type in the metadata",
					type);
			throw new OverflowProcessorException(
					"The update overflow record data type is error, update type is " + type);
		}

	}

	public void delete(String deltaObjectId, String measurementId, long timestamp, TSDataType type)
			throws OverflowProcessorException {
		if (ofSupport.delete(deltaObjectId, measurementId, timestamp, type)) {
			++recordCount;
			checkMemorySize();
		} else {
			LOGGER.error("The delete overflow record data type {} is not consistent with the type in the metadata",
					type);
			throw new OverflowProcessorException(
					"The delete overflow record data type is error, delete type is " + type);
		}

	}

	public List<Object> query(String deltaObjectId, String measurementId, SingleSeriesFilterExpression timeFilter,
			SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter) {
		return ofSupport.query(deltaObjectId, measurementId, timeFilter, freqFilter, valueFilter);
	}

	private void checkMemorySize() throws OverflowProcessorException {
		if (recordCount >= recordCountForNextMemCheck) {
			long memSize = ofSupport.calculateMemSize();
			if (memSize > memoryBlockSize) {
				flushRowGroupToStore(false);
				recordCountForNextMemCheck = Math.min(Math.max(MINIMUM_RECORD_COUNT_FOR_CHECK, recordCount / 2),
						MAXIMUM_RECORD_COUNT_FOR_CHECK);
			} else {
				float recordSize = (float) memSize / recordCount;
				recordCountForNextMemCheck = Math.min(
						Math.max(MINIMUM_RECORD_COUNT_FOR_CHECK,
								(recordCount + (long) (memoryBlockSize / recordSize)) / 2),
						recordCount + MAXIMUM_RECORD_COUNT_FOR_CHECK);
			}
		}
	}

	private void flushRowGroupToStore(boolean synchronize) throws OverflowProcessorException {
		if (recordCount > 0) {
			synchronized (flushState) {
				while (flushState.isFlushing()) {
					try {
						flushState.wait();
					} catch (InterruptedException e) {
						LOGGER.error("Waiting the flushstate error in flush row group to store. reason {}",
								e.getMessage());
						// continue to wait
					}
				}
			}
			
			try {
				WriteLogManager.getInstance().startOverflowFlush(nameSpacePath);
			} catch (IOException e1) {
				throw new OverflowProcessorException(e1);
			}
			
			ofSupport.switchWorkToFlush();
			recordCount = 0;
			// update the status of the newIntervalFiles
			try {
				// backup newIntervalFile list and emptyIntervalFileNode
				overflowFlushAction.act();
				// backup overflowNameSpaceSet
				filenodeManagerBackUpAction.act();
			} catch (Exception e) {
				LOGGER.error("Flush the overflow rowGroup to file faied, when overflowFlushAction act");
				e.printStackTrace();
				throw new OverflowProcessorException(
						"Flush the overflow rowGroup to file faied, when overflowFlushAction act");
			}
			if (synchronize) {
				// flush overflow row group synchronously
				// just close overflow processor will call this function by
				// using true parameter
				flushState.setFlushing();
				try {
					// flush overflow rowgroup data
					ofSupport.flushRowGroupToStore();
					// store the rowgroup metadata to file
					writeStoreToDisk(-1, false);
					// call filenode function to update intervalFile list
					filenodeFlushAction.act();
					// call filenode manager function to flush overflow
					// nameSpacePath set
					filenodeManagerFlushAction.act();
					WriteLogManager.getInstance().endOverflowFlush(nameSpacePath);
				} catch (IOException e) {
					LOGGER.error("Flush overflow rowGroup to file failed synchronously");
					throw new OverflowProcessorException(
							"Flush overflow rowGroup to file failed synchronously, reason:" + e.getMessage());
				} catch (OverflowProcessorException e) {
					LOGGER.error("Flush overflow rowgroup restore failed, the reason is {}", e.getMessage());
					System.exit(0);
				} catch (Exception e) {
					LOGGER.error("filenodeFlushAction action failed");
					e.printStackTrace();
					throw new OverflowProcessorException("FilenodeFlushAction action failed");
				} finally {
					synchronized (flushState) {
						flushState.setUnFlushing();
						flushState.notify();
					}
				}
			} else {
				// flush overflow row group asynchronously
				flushState.setFlushing();
				Thread AsynflushThread = new Thread() {
					@Override
					public void run() {
						try {
							// flush overflow rowgroup data
							ofSupport.flushRowGroupToStore();
							// store the rowgorup metadata to file
							writeStoreToDisk(-1, false);
							// call filenode function to update intervalFile
							// list
							filenodeFlushAction.act();
							// call filenode manager function to flush overflow
							// nameSpacePath set
							filenodeManagerFlushAction.act();
							WriteLogManager.getInstance().endOverflowFlush(nameSpacePath);
						} catch (IOException e) {
							LOGGER.error("Flush overflow rowgroup to file error in asynchronously. The reason is {}",
									e.getMessage());
							e.printStackTrace();
						} catch (OverflowProcessorException e) {
							LOGGER.error("Flush overflow rowgroup restore failed, the reason is {}", e.getMessage());
							System.exit(0);
						} catch (Exception e) {
							LOGGER.error("filenodeFlushAction action failed");
							e.printStackTrace();
						} finally {
							synchronized (flushState) {
								flushState.setUnFlushing();
								flushState.notify();
							}
						}
					}
				};
				AsynflushThread.start();
			}
		}
	}

	public String getFileName() {
		return fileName;
	}

	@Override
	public boolean canBeClosed() {
		return !isMerging && !flushState.isFlushing();
	}

	@Override
	public void close() throws OverflowProcessorException {
		LOGGER.info("Start to close overflow processor, the nameSpacePath is {}", nameSpacePath);
		try {
			flushRowGroupToStore(true);
		} catch (OverflowProcessorException e) {
			LOGGER.error("Close the overflow processor error, the nameSpacePath is {}", nameSpacePath);
			throw new OverflowProcessorException(
					String.format("Close the bufferwrite processor error, the nameSpacePath is %s, the reason is %s",
							nameSpacePath, e.getMessage()));
		}
		long lastUpdateOffset = -1L;
		try {
			lastUpdateOffset = ofSupport.endFile();
		} catch (IOException e) {
			LOGGER.error("Get the last update time failed, the nameSpacePath is {}", nameSpacePath);
			throw new OverflowProcessorException(
					String.format("End the overflow file failed, the nameSpacePath is %s, the reason is %s",
							nameSpacePath, e.getMessage()));
		}
		if (lastUpdateOffset != -1) {
			writeStoreToDisk(lastUpdateOffset, true);
		} else {
			LOGGER.warn("Close the overflow processor, but no overflow metadata was flush");
		}
	}

	public void switchWorkingToMerge() throws OverflowProcessorException {
		synchronized (flushState) {
			while (flushState.isFlushing()) {
				try {
					flushState.wait();
				} catch (InterruptedException e) {
					LOGGER.error("Waiting the flushstate error in switch overflow to merger. reason {}",
							e.getMessage());
					e.printStackTrace();
				}
			}
		}
		isMerging = true;
		try {
			ofSupport.switchWorkToMerge();
		} catch (IOException e) {
			LOGGER.error("SwitchFileIOToMerge failed, reason:{}", e.getMessage());
			throw new OverflowProcessorException(
					"Switch overflow from working to merge error, reason: " + e.getMessage());
		}
	}

	public void switchMergeToWorking() throws OverflowProcessorException {
		try {
			ofSupport.switchMergeToWork();
		} catch (IOException e) {
			LOGGER.error("Switch overflow from merge to working error, reason: {}", e.getMessage());
			throw new OverflowProcessorException(
					"Switch overflow from merge to working error, reason: " + e.getMessage());
		} finally {
			isMerging = false;
		}
	}

	/**
	 * convert String to byte array
	 * 
	 * @return result byte array
	 */
	private byte[] convertStringToBytes(TSDataType type, String o) {
		switch (type) {
		case INT32:
			return BytesUtils.intToBytes(Integer.valueOf(o));
		case INT64:
			return BytesUtils.longToBytes(Long.valueOf(o));
		case BOOLEAN:
			return BytesUtils.boolToBytes(Boolean.valueOf(o));
		case FLOAT:
			return BytesUtils.floatToBytes(Float.valueOf(o));
		case DOUBLE:
			return BytesUtils.doubleToBytes(Double.valueOf(o));
		case BYTE_ARRAY:
			return BytesUtils.StringToBytes(o);
		default:
			LOGGER.error("unsupport data type: {}", type);
			throw new UnsupportedOperationException();
		}
	}

	private class OverflowStoreStruct {
		public final long lastOverflowFilePosition;
		public final long lastOverflowRowGroupPosition;
		public final OFFileMetadata ofFileMetadata;

		public OverflowStoreStruct(long lastOverflowFilePosition, long lastOverflowRowGroupPosition,
				OFFileMetadata ofFileMetadata) {
			super();
			this.lastOverflowFilePosition = lastOverflowFilePosition;
			this.lastOverflowRowGroupPosition = lastOverflowRowGroupPosition;
			this.ofFileMetadata = ofFileMetadata;
		}
	}
}
