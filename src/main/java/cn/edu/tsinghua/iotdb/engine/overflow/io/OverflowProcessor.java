package cn.edu.tsinghua.iotdb.engine.overflow.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;


import cn.edu.tsinghua.iotdb.engine.memcontrol.BasicMemController;
import cn.edu.tsinghua.iotdb.utils.MemUtils;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.iotdb.engine.flushthread.FlushManager;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.Processor;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.Action;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.FileNodeConstants;
import cn.edu.tsinghua.iotdb.engine.overflow.metadata.OFFileMetadata;
import cn.edu.tsinghua.iotdb.engine.overflow.utils.OverflowReadWriteThriftFormatUtils;
import cn.edu.tsinghua.iotdb.engine.overflow.utils.TSFileMetaDataConverter;
import cn.edu.tsinghua.iotdb.engine.overflow.utils.TimePair;
import cn.edu.tsinghua.iotdb.engine.utils.FlushState;
import cn.edu.tsinghua.iotdb.exception.OverflowProcessorException;
import cn.edu.tsinghua.iotdb.sys.writelog.WriteLogManager;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;

public class OverflowProcessor extends Processor {

	private static final Logger LOGGER = LoggerFactory.getLogger(OverflowProcessor.class);
	private static final TsfileDBConfig TsFileDBConf = TsfileDBDescriptor.getInstance().getConfig();

	private static final int MINIMUM_RECORD_COUNT_FOR_CHECK = 100;
	private static final int MAXIMUM_RECORD_COUNT_FOR_CHECK = 10000;
	private long recordCount = 0;
	private long recordCountForNextMemCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;
	private long memUsed = 0;

	private OverflowSupport ofSupport;
	private final int memoryBlockSize = TSFileDescriptor.getInstance().getConfig().groupSizeInByte;

	private volatile boolean isMerging = false;
	private volatile FlushState flushState = new FlushState();

	private static final String storeFileName = ".overflow";
	private static final String restoreFileName = ".restore";
	private String fileName;
	private String overflowRetoreFilePath;
	private String overflowOutputFilePath;
	private Action overflowFlushAction = null;
	private Action filenodeFlushAction = null;
	private Action filenodeManagerBackUpAction = null;
	private Action filenodeManagerFlushAction = null;
	private long lastFlushTime = -1;

	public OverflowProcessor(String processorName, Map<String, Object> parameters) throws OverflowProcessorException {
		super(processorName);
		String overflowDirPath = TsFileDBConf.overflowDataDir;
		if (overflowDirPath.length() > 0
				&& overflowDirPath.charAt(overflowDirPath.length() - 1) != File.separatorChar) {
			overflowDirPath = overflowDirPath + File.separatorChar;
		}
		// overflow data dir
		String dataPath = overflowDirPath + processorName;
		File dataDir = new File(dataPath);
		if (!dataDir.exists()) {
			dataDir.mkdirs();
			LOGGER.warn("The overflow processor data dir doesn't exists, and mkdir the dir {}",
					dataDir.getPath());
		}
		// overflow file name in the overflow data dir with the special
		// processorName.overflow
		fileName = processorName + storeFileName;
		overflowOutputFilePath = new File(dataDir, fileName).getPath();
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
			LOGGER.error("Can't get the overflowReadWrite, the overflow is {}", processorName);
			throw new OverflowProcessorException(e);
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
			LOGGER.error("Can't get the overflowFileIO, the overflow is {}", processorName);
			throw new OverflowProcessorException(e);
		}
		// create overflow supoort
		try {
			this.ofSupport = new OverflowSupport(overflowFileIO, ofFileMetadata);
		} catch (IOException e) {
			LOGGER.error("Can't get the overflowSupport, the overflow is {}", processorName);
			throw new OverflowProcessorException(e);
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
					OverflowReadWriteThriftFormatUtils.writeOFFileMetaData(
							metadataConverter.toThriftOFFileMetadata(0, fileMetadata), fileOutputStream);

				} else {
					fileOutputStream.write(BytesUtils.longToBytes(lastOverflowRowGroupPostion));
				}

			} catch (IOException e) {
				LOGGER.error("Flush the information for the overflow processor error, the overflow is {}",
						getProcessorName());
				throw new OverflowProcessorException(e);
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
				throw new OverflowProcessorException(e);
			}
			int off = 0;
			int len = buff.length - off;
			cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFFileMetadata thriftfileMetadata = null;
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
				thriftfileMetadata = OverflowReadWriteThriftFormatUtils.readOFFileMetaData(fileInputStream);
				TSFileMetaDataConverter metadataConverter = new TSFileMetaDataConverter();
				OFFileMetadata ofFileMetadata = metadataConverter.toOFFileMetadata(thriftfileMetadata);
				return new OverflowStoreStruct(lastOverflowFilePosition, lastOverflowRowGroupPosition, ofFileMetadata);
			} catch (IOException e) {
				LOGGER.error(
						"Read the data: lastOverflowFilePostion, lastOverflowRowGroupPostion, offilemetadata error");
				throw new OverflowProcessorException(e);
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

	// to unify with Methods in BufferWriteProcessor
	public void insert(String deltaObjectId, TSRecord record) throws OverflowProcessorException {
		long newUsage = memUsed;
		BasicMemController.UsageLevel level = BasicMemController.getInstance().reportUse(this, newUsage);
		switch (level) {
			case SAFE:
				for (DataPoint dataPoint : record.dataPointList) {
					insert(deltaObjectId, dataPoint.getMeasurementId(), record.time,
							dataPoint.getType(), dataPoint.getValue().toString());
				}
				memUsed += newUsage;
				break;
			case WARNING:
				LOGGER.debug("Memory usage will exceed warning threshold, current : {}." ,
						MemUtils.bytesCntToStr(BasicMemController.getInstance().getTotalUsage()));
				for (DataPoint dataPoint : record.dataPointList) {
					insert(deltaObjectId, dataPoint.getMeasurementId(), record.time,
							dataPoint.getType(), dataPoint.getValue().toString());
				}
				memUsed += newUsage;
				break;
			case DANGEROUS:
				LOGGER.warn("Memory usage will exceed dangerous threshold, current : {}." ,
						MemUtils.bytesCntToStr(BasicMemController.getInstance().getTotalUsage()));
				throw new OverflowProcessorException("Memory usage exceeded dangerous threshold.");
			default:
		}
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
			SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter, TSDataType dataType) {
		return ofSupport.query(deltaObjectId, measurementId, timeFilter, freqFilter, valueFilter, dataType);
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
		if (lastFlushTime > 0) {
			long thisFLushTime = System.currentTimeMillis();
			DateTime lastDateTime = new DateTime(lastFlushTime, TsfileDBDescriptor.getInstance().getConfig().timeZone);
			DateTime thisDateTime = new DateTime(thisFLushTime, TsfileDBDescriptor.getInstance().getConfig().timeZone);
			LOGGER.info("Last flush time is {}, this flush time is {}, flush time interval is {}", lastDateTime,
					thisDateTime, thisFLushTime - lastFlushTime);
			lastFlushTime = thisFLushTime;
		}
		if (recordCount > 0) {
			synchronized (flushState) {
				while (flushState.isFlushing()) {
					try {
						flushState.wait();
					} catch (InterruptedException e) {
						LOGGER.error("Waiting the flushstate error in flush row group to store.", e);
						// continue to wait
					}
				}
			}
			long oldMemUsage = memUsed;
			memUsed = 0;
			if (TsfileDBDescriptor.getInstance().getConfig().enableWal) {
				try {
					WriteLogManager.getInstance().startOverflowFlush(getProcessorName());
				} catch (IOException e1) {
					throw new OverflowProcessorException(e1);
				}
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
				throw new OverflowProcessorException(e);
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
					// processorName set
					filenodeManagerFlushAction.act();
					if (TsfileDBDescriptor.getInstance().getConfig().enableWal) {
						WriteLogManager.getInstance().endOverflowFlush(getProcessorName());
					}
				} catch (IOException e) {
					LOGGER.error("Flush overflow rowGroup to file failed synchronously");
					throw new OverflowProcessorException(e);
				} catch (OverflowProcessorException e) {
					LOGGER.error("Flush overflow rowgroup restore failed.", e);
					System.exit(0);
				} catch (Exception e) {
					LOGGER.error("filenodeFlushAction action failed");
					throw new OverflowProcessorException(e);
				} finally {
					synchronized (flushState) {
						flushState.setUnFlushing();
						flushState.notify();
					}
				}
				BasicMemController.getInstance().reportFree(this, oldMemUsage);
				checkSize();
			} else {
				// flush overflow row group asynchronously
				flushState.setFlushing();
				Runnable AsynflushThread = () -> {
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
						if (TsfileDBDescriptor.getInstance().getConfig().enableWal) {
							WriteLogManager.getInstance().endOverflowFlush(getProcessorName());
						}
					} catch (IOException e) {
						LOGGER.error("Flush overflow rowgroup to file error in asynchronously.", e);
					} catch (OverflowProcessorException e) {
						LOGGER.error("Flush overflow rowgroup restore failed.", e);
						System.exit(0);
					} catch (Exception e) {
						LOGGER.error("filenodeFlushAction action failed.", e);
					} finally {
						synchronized (flushState) {
							flushState.setUnFlushing();
							flushState.notify();
						}
						BasicMemController.getInstance().reportFree(this, oldMemUsage);
						checkSize();
					}
				};
				FlushManager.getInstance().submit(AsynflushThread);
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
	public void flush() throws IOException{
		try {
			flushRowGroupToStore(false);
		} catch (OverflowProcessorException e) {
			e.printStackTrace();
			throw new IOException(e);
		}
	}
	
	@Override
	public void close() throws OverflowProcessorException {
		LOGGER.info("Start to close overflow processor, the overflow is {}", getProcessorName());
		try {
			flushRowGroupToStore(true);
		} catch (OverflowProcessorException e) {
			LOGGER.error("Close the overflow processor error, the overflow is {}", getProcessorName());
			throw new OverflowProcessorException(e);
		}
		long lastUpdateOffset = -1L;
		try {
			lastUpdateOffset = ofSupport.endFile();
		} catch (IOException e) {
			LOGGER.error("Get the last update time failed, the overflow is {}", getProcessorName());
			throw new OverflowProcessorException(e);
		}
		if (lastUpdateOffset != -1) {
			writeStoreToDisk(lastUpdateOffset, true);
		} else {
			LOGGER.warn("Close the overflow processor, but no overflow metadata was flush");
		}
	}
	
	@Override
	public long memoryUsage(){
		
		return  ofSupport.calculateMemSize();
	}

	public void switchWorkingToMerge() throws OverflowProcessorException {
		synchronized (flushState) {
			while (flushState.isFlushing()) {
				try {
					flushState.wait();
				} catch (InterruptedException e) {
					LOGGER.error("Waiting the flushstate error in switch overflow to merge.", e);
				}
			}
		}
		isMerging = true;
		try {
			ofSupport.switchWorkToMerge();
		} catch (IOException e) {
			LOGGER.error("SwitchFileIOToMerge failed.");
			throw new OverflowProcessorException(e);
		}
	}

	public void switchMergeToWorking() throws OverflowProcessorException {
		try {
			ofSupport.switchMergeToWork();
		} catch (IOException e) {
			LOGGER.error("Switch overflow from merge to working error");
			throw new OverflowProcessorException(e);
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
		case TEXT:
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

	/**
	 * @return The sum of all timeseries's metadata size within this file.
	 */
	public long getMetaSize() {
		// TODO : [MemControl] implement this
		return 0;
	}

	/**
	 * @return The file size of the OverflowFile corresponding to this processor.
	 */
	public long getFileSize() {
		// TODO : save this variable to avoid object creation?
		File file = new File(overflowOutputFilePath);
		return file.length();
	}

	/**
	 * Close current OverflowFile and open a new one for future writes.
	 * Block new writes and wait until current writes finish.
	 */
	public void rollToNewFile() {
		// TODO : [MemControl] implement this
	}

	/**
	 * Check if this OverflowFile has too big metadata or file.
	 * If true, close current file and open a new one.
	 */
	private void checkSize() {
		TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
		long metaSize = getMetaSize();
		long fileSize = getFileSize();
		if(metaSize >= config.bufferwriteMetaSizeThreshold ||
				fileSize >= config.bufferwriteFileSizeThreshold) {
			LOGGER.info("{} size reaches threshold, closing. meta size is {}, file size is {}",
					this.fileName, MemUtils.bytesCntToStr(metaSize), MemUtils.bytesCntToStr(fileSize));
			rollToNewFile();
		}
	}
}
