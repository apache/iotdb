package cn.edu.tsinghua.iotdb.engine.bufferwrite;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import cn.edu.tsinghua.iotdb.conf.TsFileDBConstant;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.writelog.manager.MultiFileLogNodeManager;
import cn.edu.tsinghua.iotdb.writelog.node.WriteLogNode;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.Processor;
import cn.edu.tsinghua.iotdb.engine.memcontrol.BasicMemController;
import cn.edu.tsinghua.iotdb.engine.memtable.IMemTable;
import cn.edu.tsinghua.iotdb.engine.memtable.MemSeriesLazyMerger;
import cn.edu.tsinghua.iotdb.engine.memtable.MemTableFlushUtil;
import cn.edu.tsinghua.iotdb.engine.memtable.PrimitiveMemTable;
import cn.edu.tsinghua.iotdb.engine.pool.FlushManager;
import cn.edu.tsinghua.iotdb.engine.querycontext.RawSeriesChunk;
import cn.edu.tsinghua.iotdb.engine.querycontext.RawSeriesChunkLazyLoadImpl;
import cn.edu.tsinghua.iotdb.engine.utils.FlushStatus;
import cn.edu.tsinghua.iotdb.exception.BufferWriteProcessorException;
import cn.edu.tsinghua.iotdb.utils.MemUtils;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TsRowGroupBlockMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.format.RowGroupBlockMetaData;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;

/**
 * @author liukun
 */
public class BufferWriteProcessor extends Processor {

	private static final Logger LOGGER = LoggerFactory.getLogger(BufferWriteProcessor.class);
	private static final TsfileDBConfig TsFileDBConf = TsfileDBDescriptor.getInstance().getConfig();
	private static final TSFileConfig TsFileConf = TSFileDescriptor.getInstance().getConfig();
	private static final int TS_METADATA_BYTE_SIZE = 4;
	private static final int TSFILE_POSITION_BYTE_SIZE = 8;

	private volatile FlushStatus flushStatus = new FlushStatus();
	private ReentrantLock flushQueryLock = new ReentrantLock();

	private IMemTable workMemTable;
	private IMemTable flushMemTable;

	private FileSchema fileSchema;
	private BufferWriteIOWriter bufferIOWriter;
	private int lastRowgroupSize = 0;

	// this just the bufferwrite file name
	private String baseDir;
	private String fileName;
	private static final String restoreFile = ".restore";
	// this is the bufferwrite file absolute path
	private String bufferwriteRestoreFilePath;
	private String bufferwriteOutputFilePath;
	private String bufferwriterelativePath;
	private File bufferwriteOutputFile;

	private boolean isNewProcessor = false;

	private Action bufferwriteFlushAction = null;
	private Action bufferwriteCloseAction = null;
	private Action filenodeFlushAction = null;

	private long memThreshold = TsFileConf.groupSizeInByte;
	private long lastFlushTime = -1;
	private long valueCount = 0;
	private volatile boolean isFlush;
	private AtomicLong memSize = new AtomicLong();

	private WriteLogNode logNode;

	public BufferWriteProcessor(String baseDir, String processorName, String fileName, Map<String, Object> parameters,
			FileSchema fileSchema) throws BufferWriteProcessorException {
		super(processorName);

		this.fileName = fileName;
		String restoreFileName = fileName + restoreFile;

		this.baseDir = baseDir;
		if (baseDir.length() > 0
				&& baseDir.charAt(baseDir.length() - 1) != File.separatorChar) {
			baseDir = baseDir + File.separatorChar;
		}
		String dataDirPath = baseDir + processorName;
		File dataDir = new File(dataDirPath);
		if (!dataDir.exists()) {
			dataDir.mkdirs();
			LOGGER.debug("The bufferwrite processor data dir doesn't exists, create new directory {}.", dataDirPath);
		}
		bufferwriteOutputFile = new File(dataDir, fileName);
		File restoreFile = new File(dataDir, restoreFileName);
		bufferwriteRestoreFilePath = restoreFile.getPath();
		bufferwriteOutputFilePath = bufferwriteOutputFile.getPath();
		bufferwriterelativePath = processorName + File.separatorChar + fileName;
		// get the fileschema
		this.fileSchema = fileSchema;

		if (bufferwriteOutputFile.exists() && restoreFile.exists()) {
			//
			// There is one damaged file, and the RESTORE_FILE_SUFFIX exist
			//
			LOGGER.info("Recorvery the bufferwrite processor {}.", processorName);
			bufferwriteRecovery();

		} else {

			ITsRandomAccessFileWriter outputWriter;
			try {
				outputWriter = new TsRandomAccessFileWriter(bufferwriteOutputFile);
			} catch (IOException e) {
				LOGGER.error("Construct the TSRandomAccessFileWriter error, the absolutePath is {}.",
						bufferwriteOutputFile.getPath(), e);
				throw new BufferWriteProcessorException(e);
			}

			try {
				bufferIOWriter = new BufferWriteIOWriter(outputWriter);
			} catch (IOException e) {
				LOGGER.error("Get the BufferWriteIOWriter error, the bufferwrite is {}.", processorName, e);
				throw new BufferWriteProcessorException(e);
			}
			isNewProcessor = true;
			// write restore file
			writeStoreToDisk();
		}
		// init action
		// the action from the corresponding filenode processor
		bufferwriteFlushAction = (Action) parameters.get(FileNodeConstants.BUFFERWRITE_FLUSH_ACTION);
		bufferwriteCloseAction = (Action) parameters.get(FileNodeConstants.BUFFERWRITE_CLOSE_ACTION);
		filenodeFlushAction = (Action) parameters.get(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION);
		workMemTable = new PrimitiveMemTable();

		if (TsfileDBDescriptor.getInstance().getConfig().enableWal) {
			try {
				logNode = MultiFileLogNodeManager.getInstance().getNode(
						processorName + TsFileDBConstant.BUFFERWRITE_LOG_NODE_SUFFIX, getBufferwriteRestoreFilePath(),
						FileNodeManager.getInstance().getRestoreFilePath(processorName));
			} catch (IOException e) {
				throw new BufferWriteProcessorException(e);
			}
		}
	}

	/**
	 * <p>
	 * Recovery the bufferwrite status.<br>
	 * The one part is the last intervalFile<br>
	 * The other part is all the intervalFile, and other file will be deleted
	 * </p>
	 *
	 * @throws BufferWriteProcessorException
	 */
	private void bufferwriteRecovery() throws BufferWriteProcessorException {

		Pair<Long, List<RowGroupMetaData>> pair;
		try {
			pair = readStoreFromDisk();
		} catch (IOException e) {
			LOGGER.error("Failed to read bufferwrite {} restore file.", getProcessorName());
			throw new BufferWriteProcessorException(e);
		}
		ITsRandomAccessFileWriter output;
		long lastFlushPosition = pair.left;
		File lastBufferWriteFile = new File(bufferwriteOutputFilePath);
		if (lastBufferWriteFile.length() != lastFlushPosition) {
			LOGGER.warn(
					"The last bufferwrite file {} is damaged, the length of the last bufferwrite file is {}, the end of last successful flush is {}.",
					lastBufferWriteFile.getPath(), lastBufferWriteFile.length(), lastFlushPosition);
			try {
				FileChannel fileChannel = new FileOutputStream(bufferwriteOutputFile, true).getChannel();
				fileChannel.truncate(lastFlushPosition);
				fileChannel.close();
				//cutOffFile(lastFlushPosition);
			} catch (IOException e) {
				LOGGER.error(
						"Cut off damaged file error, the damaged file path is {}, the length is {}, the cut off length is {}.",
						bufferwriteOutputFilePath, lastBufferWriteFile.length(), lastFlushPosition, e);
				throw new BufferWriteProcessorException(e);
			}
		}
		try {
			// Notice: the offset is seek to end of the file by API of kr
			output = new TsRandomAccessFileWriter(lastBufferWriteFile);
		} catch (IOException e) {
			LOGGER.error("Can't construct the RandomAccessOutputStream, the outputPath is {}.",
					bufferwriteOutputFilePath);
			throw new BufferWriteProcessorException(e);
		}
		try {
			// Notice: the parameter of lastPosition is not used beacuse of the
			// API of kr
			bufferIOWriter = new BufferWriteIOWriter(output, lastFlushPosition, pair.right);
		} catch (IOException e) {
			LOGGER.error("Can't get the BufferWriteIOWriter while recoverying, the bufferwrite processor is {}.",
					getProcessorName(), e);
			throw new BufferWriteProcessorException(e);
		}
		isNewProcessor = false;
	}

	private void cutOffFile(long length) throws IOException {

		String tempPath = bufferwriteOutputFilePath + ".backup";
		File tempFile = new File(tempPath);
		File normalFile = new File(bufferwriteOutputFilePath);

		if (normalFile.exists() && normalFile.length() > 0) {

			if (tempFile.exists()) {
				tempFile.delete();
			}
			RandomAccessFile normalReader = null;
			RandomAccessFile tempWriter = null;
			try {
				normalReader = new RandomAccessFile(normalFile, "r");
				tempWriter = new RandomAccessFile(tempFile, "rw");
			} catch (FileNotFoundException e) {
				LOGGER.error(
						"Can't get the RandomAccessFile read and write, the normal path is {}, the temp path is {}.",
						bufferwriteOutputFilePath, tempPath);
				if (normalReader != null) {
					normalReader.close();
				}
				if (tempWriter != null) {
					tempWriter.close();
				}
				throw e;
			}
			long offset = 0;
			int step = 4 * 1024 * 1024;
			byte[] buff = new byte[step];
			while (length - offset >= step) {
				try {
					normalReader.readFully(buff);
					tempWriter.write(buff);
				} catch (IOException e) {
					LOGGER.error("normalReader read data failed or tempWriter write data error.");
					throw e;
				}
				offset = offset + step;
			}
			normalReader.readFully(buff, 0, (int) (length - offset));
			tempWriter.write(buff, 0, (int) (length - offset));
			normalReader.close();
			tempWriter.close();
		}
		normalFile.delete();
		tempFile.renameTo(normalFile);
	}

	/**
	 * This is only used after flush one rowroup data successfully.
	 *
	 * @throws BufferWriteProcessorException
	 */
	private void writeStoreToDisk() throws BufferWriteProcessorException {

		long lastPosition;
		try {
			lastPosition = bufferIOWriter.getPos();
		} catch (IOException e) {
			LOGGER.error("Can't get the bufferwrite io position, the buffewrite processor is {}", getProcessorName(),
					e);
			throw new BufferWriteProcessorException(e);
		}
		List<RowGroupMetaData> rowGroupMetaDatas = bufferIOWriter.getRowGroups();
		List<RowGroupMetaData> appendMetadata = new ArrayList<>();
		for (int i = lastRowgroupSize; i < rowGroupMetaDatas.size(); i++) {
			appendMetadata.add(rowGroupMetaDatas.get(i));
		}
		lastRowgroupSize = rowGroupMetaDatas.size();
		TsRowGroupBlockMetaData tsRowGroupBlockMetaData = new TsRowGroupBlockMetaData();
		tsRowGroupBlockMetaData.setRowGroups(appendMetadata);

		RandomAccessFile out = null;
		try {
			out = new RandomAccessFile(bufferwriteRestoreFilePath, "rw");
		} catch (FileNotFoundException e) {
			LOGGER.error("The restore file {} can't be created, the bufferwrite processor is {}",
					bufferwriteRestoreFilePath, getProcessorName(), e);
			throw new BufferWriteProcessorException(e);
		}
		try {
			if (out.length() > 0) {
				out.seek(out.length() - TSFILE_POSITION_BYTE_SIZE);
			}
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ReadWriteThriftFormatUtils.writeRowGroupBlockMetadata(tsRowGroupBlockMetaData.convertToThrift(), baos);
			// write metadata size using int
			int metadataSize = baos.size();
			out.write(BytesUtils.intToBytes(metadataSize));
			// write metadata
			out.write(baos.toByteArray());
			// write tsfile position using byte[8] which is present one long
			// number
			byte[] lastPositionBytes = BytesUtils.longToBytes(lastPosition);
			out.write(lastPositionBytes);
		} catch (IOException e) {
			throw new BufferWriteProcessorException(e);
		} finally {
			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
					e.printStackTrace();
					throw new BufferWriteProcessorException(e);
				}
			}
		}
	}

	/**
	 * This is used to delete the file which is used to restore buffer write
	 * processor. This is only used after closing the buffer write processor
	 * successfully.
	 */
	private void deleteRestoreFile() {
		File restoreFile = new File(bufferwriteRestoreFilePath);
		if (restoreFile.exists()) {
			restoreFile.delete();
		}
	}

	/**
	 * The left of the pair is the last successful flush position. The right of
	 * the pair is the rowGroupMetadata.
	 *
	 * @return - the left is the end position of the last rowgroup flushed, the
	 *         right is all the rowgroup meatdata flushed
	 * @throws IOException
	 */
	private Pair<Long, List<RowGroupMetaData>> readStoreFromDisk() throws IOException {
		byte[] lastPostionBytes = new byte[TSFILE_POSITION_BYTE_SIZE];
		List<RowGroupMetaData> groupMetaDatas = new ArrayList<>();
		RandomAccessFile randomAccessFile = null;
		try {
			randomAccessFile = new RandomAccessFile(bufferwriteRestoreFilePath, "rw");
			long fileLength = randomAccessFile.length();
			// read tsfile position
			long point = randomAccessFile.getFilePointer();
			while (point + TSFILE_POSITION_BYTE_SIZE < fileLength) {
				byte[] metadataSizeBytes = new byte[TS_METADATA_BYTE_SIZE];
				randomAccessFile.read(metadataSizeBytes);
				int metadataSize = BytesUtils.bytesToInt(metadataSizeBytes);
				byte[] thriftBytes = new byte[metadataSize];
				randomAccessFile.read(thriftBytes);
				ByteArrayInputStream inputStream = new ByteArrayInputStream(thriftBytes);
				RowGroupBlockMetaData rowGroupBlockMetaData = ReadWriteThriftFormatUtils
						.readRowGroupBlockMetaData(inputStream);
				TsRowGroupBlockMetaData blockMeta = new TsRowGroupBlockMetaData();
				blockMeta.convertToTSF(rowGroupBlockMetaData);
				groupMetaDatas.addAll(blockMeta.getRowGroups());
				lastRowgroupSize = groupMetaDatas.size();
				point = randomAccessFile.getFilePointer();
			}
			// read the tsfile position information using byte[8] which is
			// present one long number.
			randomAccessFile.read(lastPostionBytes);
		} catch (FileNotFoundException e) {
			LOGGER.error("The restore file does not exist, the restore file path is {}.", bufferwriteRestoreFilePath,
					e);
			throw e;
		} catch (IOException e) {
			LOGGER.error("Read data from file error.", e);
			throw e;
		} finally {
			if (randomAccessFile != null) {
				randomAccessFile.close();
			}
		}
		long lastPostion = BytesUtils.bytesToLong(lastPostionBytes);
		Pair<Long, List<RowGroupMetaData>> result = new Pair<Long, List<RowGroupMetaData>>(lastPostion, groupMetaDatas);
		return result;
	}

	public String getFileName() {
		return fileName;
	}

	public String getBaseDir() { return baseDir; }

	public String getFileRelativePath() {
		return bufferwriterelativePath;
	}

	public boolean isNewProcessor() {
		return isNewProcessor;
	}

	public void setNewProcessor(boolean isNewProcessor) {
		this.isNewProcessor = isNewProcessor;
	}

	/**
	 * write one data point to the bufferwrite
	 * 
	 * @param deltaObjectId
	 * @param measurementId
	 * @param timestamp
	 * @param dataType
	 * @param value
	 * @return true -the size of tsfile or metadata reaches to the threshold.
	 *         false -otherwise
	 * @throws BufferWriteProcessorException
	 */
	public boolean write(String deltaObjectId, String measurementId, long timestamp, TSDataType dataType, String value)
			throws BufferWriteProcessorException {
		TSRecord record = new TSRecord(timestamp, deltaObjectId);
		DataPoint dataPoint = DataPoint.getDataPoint(dataType, measurementId, value);
		record.addTuple(dataPoint);
		return write(record);
	}

	public boolean write(TSRecord tsRecord) throws BufferWriteProcessorException {
		long memUage = MemUtils.getRecordSize(tsRecord);
		BasicMemController.UsageLevel level = BasicMemController.getInstance().reportUse(this, memUage);

		for (DataPoint dataPoint : tsRecord.dataPointList) {
			workMemTable.write(tsRecord.deltaObjectId, dataPoint.getMeasurementId(), dataPoint.getType(), tsRecord.time,
					dataPoint.getValue().toString());
		}
		valueCount++;
		switch (level) {
		case SAFE:
			// memUsed += newMemUsage;
			// memtable
			memUage = memSize.addAndGet(memUage);
			if (memUage > memThreshold) {
				LOGGER.info("The usage of memory {} in bufferwrite processor {} reaches the threshold {}",
						MemUtils.bytesCntToStr(memUage), getProcessorName(), MemUtils.bytesCntToStr(memThreshold));
				try {
					flush();
				} catch (IOException e) {
					e.printStackTrace();
					throw new BufferWriteProcessorException(e);
				}
			}
			return false;
		case WARNING:
			LOGGER.warn("Memory usage will exceed warning threshold, current : {}.",
					MemUtils.bytesCntToStr(BasicMemController.getInstance().getTotalUsage()));
			// memUsed += newMemUsage;
			// memtable
			memUage = memSize.addAndGet(memUage);
			if (memUage > memThreshold) {
				LOGGER.info("The usage of memory {} in bufferwrite processor {} reaches the threshold {}",
						MemUtils.bytesCntToStr(memUage), getProcessorName(), MemUtils.bytesCntToStr(memThreshold));
				try {
					flush();
				} catch (IOException e) {
					e.printStackTrace();
					throw new BufferWriteProcessorException(e);
				}
			}
			return false;
		case DANGEROUS:
		default:
			LOGGER.warn("Memory usage will exceed dangerous threshold, current : {}.",
					MemUtils.bytesCntToStr(BasicMemController.getInstance().getTotalUsage()));
			return false;
		}
	}

	@Deprecated
	public Pair<List<Object>, List<RowGroupMetaData>> queryBufferwriteData(String deltaObjectId, String measurementId) {
		flushQueryLock.lock();
		try {
			List<Object> memData = new ArrayList<>();
			List<RowGroupMetaData> list = new ArrayList<>();
			return new Pair<>(memData, list);
		} finally {
			flushQueryLock.unlock();
		}
	}

	public Pair<RawSeriesChunk, List<TimeSeriesChunkMetaData>> queryBufferwriteData(String deltaObjectId,
			String measurementId, TSDataType dataType) {
		flushQueryLock.lock();
		try {
			MemSeriesLazyMerger memSeriesLazyMerger = new MemSeriesLazyMerger();
			if (isFlush) {
				memSeriesLazyMerger.addMemSeries(flushMemTable.query(deltaObjectId, measurementId, dataType));
			}
			memSeriesLazyMerger.addMemSeries(workMemTable.query(deltaObjectId, measurementId, dataType));
			RawSeriesChunk rawSeriesChunk = new RawSeriesChunkLazyLoadImpl(dataType, memSeriesLazyMerger);
			return new Pair<>(rawSeriesChunk,
					bufferIOWriter.getCurrentTimeSeriesMetadataList(deltaObjectId, measurementId, dataType));
		} finally {
			flushQueryLock.unlock();
		}
	}

	private void switchWorkToFlush() {
		flushQueryLock.lock();
		try {
			if (flushMemTable == null) {
				flushMemTable = workMemTable;
				workMemTable = new PrimitiveMemTable();
			}
		} finally {
			isFlush = true;
			flushQueryLock.unlock();
		}
	}

	private void switchFlushToWork() {
		flushQueryLock.lock();
		try {
			flushMemTable.clear();
			flushMemTable = null;
			bufferIOWriter.addNewRowGroupMetaDataToBackUp();
		} finally {
			isFlush = false;
			flushQueryLock.unlock();
		}
	}

	private void flushOperation(String flushFunction) {
		long flushStartTime = System.currentTimeMillis();
		LOGGER.info("The bufferwrite processor {} starts flushing {}.", getProcessorName(), flushFunction);
		try {
			long startFlushDataTime = System.currentTimeMillis();
			long startPos = bufferIOWriter.getPos();
			// TODO : FLUSH DATA
			MemTableFlushUtil.flushMemTable(fileSchema, bufferIOWriter, flushMemTable);
			long flushDataSize = bufferIOWriter.getPos() - startPos;
			long timeInterval = System.currentTimeMillis() - startFlushDataTime;
			if (timeInterval == 0) {
				timeInterval = 1;
			}
			LOGGER.info("The bufferwrite processor {} flush {}, actual:{}, time consumption:{} ms, flush rate:{}/s",
					getProcessorName(), flushFunction, MemUtils.bytesCntToStr(flushDataSize), timeInterval,
					MemUtils.bytesCntToStr(flushDataSize / timeInterval * 1000));
			// write restore information
			writeStoreToDisk();
			filenodeFlushAction.act();
			if (TsfileDBDescriptor.getInstance().getConfig().enableWal) {
				logNode.notifyEndFlush(null);
			}
		} catch (IOException e) {
			LOGGER.error("The bufferwrite processor {} failed to flush {}.", getProcessorName(), flushFunction, e);
		} catch (Exception e) {
			LOGGER.error("The bufferwrite processor {} failed to flush {}, when calling the filenodeFlushAction.",
					getProcessorName(), flushFunction, e);
		} finally {
			synchronized (flushStatus) {
				flushStatus.setUnFlushing();
				switchFlushToWork();
				flushStatus.notify();
				LOGGER.info("The bufferwrite processor {} ends flushing {}.", getProcessorName(), flushFunction);
			}
		}
		// BasicMemController.getInstance().reportFree(BufferWriteProcessor.this,
		// oldMemUsage);
		long flushEndTime = System.currentTimeMillis();
		long flushInterval = flushEndTime - flushStartTime;
		DateTime startDateTime = new DateTime(flushStartTime, TsfileDBDescriptor.getInstance().getConfig().timeZone);
		DateTime endDateTime = new DateTime(flushEndTime, TsfileDBDescriptor.getInstance().getConfig().timeZone);
		LOGGER.info(
				"The bufferwrite processor {} flush {}, start time is {}, flush end time is {}, flush time consumption is {}ms",
				getProcessorName(), flushFunction, startDateTime, endDateTime, flushInterval);
	}

	private Future<?> flush(boolean synchronization) throws IOException {
		// statistic information for flush
		if (lastFlushTime > 0) {
			long thisFlushTime = System.currentTimeMillis();
			long flushTimeInterval = thisFlushTime - lastFlushTime;
			DateTime lastDateTime = new DateTime(lastFlushTime, TsfileDBDescriptor.getInstance().getConfig().timeZone);
			DateTime thisDateTime = new DateTime(thisFlushTime, TsfileDBDescriptor.getInstance().getConfig().timeZone);
			LOGGER.info(
					"The bufferwrite processor {}: last flush time is {}, this flush time is {}, flush time interval is {}s",
					getProcessorName(), lastDateTime, thisDateTime, flushTimeInterval / 1000);
		}
		lastFlushTime = System.currentTimeMillis();
		// check value count
		if (valueCount > 0) {
			// waiting for the end of last flush operation.
			synchronized (flushStatus) {
				while (flushStatus.isFlushing()) {
					try {
						flushStatus.wait();
					} catch (InterruptedException e) {
						LOGGER.error(
								"Encounter an interrupt error when waitting for the flushing, the bufferwrite processor is {}.",
								getProcessorName(), e);
					}
				}
			}
			// update the lastUpdatetime, prepare for flush
			try {
				bufferwriteFlushAction.act();
			} catch (Exception e) {
				LOGGER.error(
						"The bufferwrite processor {} failed to flush bufferwrite row group when calling the action function.",
						getProcessorName());
				throw new IOException(e);
			}
			if (TsfileDBDescriptor.getInstance().getConfig().enableWal) {
				logNode.notifyStartFlush();
			}
			valueCount = 0;
			flushStatus.setFlushing();
			switchWorkToFlush();
			BasicMemController.getInstance().reportFree(this, memSize.get());
			memSize.set(0);
			// switch
			if (synchronization) {
				flushOperation("synchronously");
			} else {
				FlushManager.getInstance().submit(new Runnable() {
					public void run() {
						flushOperation("asynchronously");
					}
				});
			}
		}
		return null;
	}

	public boolean isFlush() {
		synchronized (flushStatus) {
			return flushStatus.isFlushing();
		}
	}

	@Override
	public boolean flush() throws IOException {
		flush(false);
		return false;
	}

	@Override
	public boolean canBeClosed() {
		LOGGER.info("Check whether bufferwrite processor {} can be closed.", getProcessorName());
		synchronized (flushStatus) {
			while (flushStatus.isFlushing()) {
				LOGGER.info("The bufferite processor {} is flushing, waiting for the flush end.", getProcessorName());
				try {
					flushStatus.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		return true;
	}

	@Override
	public void close() throws BufferWriteProcessorException {
		try {
			long closeStartTime = System.currentTimeMillis();
			// flush data
			flush(true);
			// end file
			bufferIOWriter.endFile(fileSchema);
			// update the intervalfile for interval list
			bufferwriteCloseAction.act();
			// flush the changed information for filenode
			filenodeFlushAction.act();
			// delete the restore for this bufferwrite processor
			deleteRestoreFile();
			long closeEndTime = System.currentTimeMillis();
			long closeInterval = closeEndTime - closeStartTime;
			DateTime startDateTime = new DateTime(closeStartTime,
					TsfileDBDescriptor.getInstance().getConfig().timeZone);
			DateTime endDateTime = new DateTime(closeEndTime, TsfileDBDescriptor.getInstance().getConfig().timeZone);
			LOGGER.info(
					"Close bufferwrite processor {}, the file name is {}, start time is {}, end time is {}, time consumption is {}ms",
					getProcessorName(), fileName, startDateTime, endDateTime, closeInterval);
		} catch (IOException e) {
			LOGGER.error("Close the bufferwrite processor error, the bufferwrite is {}.", getProcessorName(), e);
			throw new BufferWriteProcessorException(e);
		} catch (Exception e) {
			LOGGER.error("Failed to close the bufferwrite processor when calling the action function.", e);
			throw new BufferWriteProcessorException(e);
		}
	}

	@Override
	public long memoryUsage() {
		return memSize.get();
	}

	/**
	 * @return The sum of all timeseries's metadata size within this file.
	 */
	public long getMetaSize() {
		// TODO : [MemControl] implement this
		return 0;
	}

	/**
	 * @return The file size of the TsFile corresponding to this processor.
	 * @throws IOException
	 */
	public long getFileSize() {
		return bufferwriteOutputFile.length() + memoryUsage();
	}

	/**
	 * Close current TsFile and open a new one for future writes. Block new
	 * writes and wait until current writes finish.
	 */
	public void rollToNewFile() {
		// TODO : [MemControl] implement this
	}

	/**
	 * Check if this TsFile has too big metadata or file. If true, close current
	 * file and open a new one.
	 * 
	 * @throws IOException
	 */
	private boolean checkSize() throws IOException {
		TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
		long metaSize = getMetaSize();
		long fileSize = getFileSize();
		if (metaSize >= config.bufferwriteMetaSizeThreshold || fileSize >= config.bufferwriteFileSizeThreshold) {
			LOGGER.info(
					"The bufferwrite processor {}, size({}) of the file {} reaches threshold {}, size({}) of metadata reaches threshold {}.",
					getProcessorName(), MemUtils.bytesCntToStr(fileSize), this.fileName,
					MemUtils.bytesCntToStr(config.bufferwriteFileSizeThreshold), MemUtils.bytesCntToStr(metaSize),
					MemUtils.bytesCntToStr(config.bufferwriteFileSizeThreshold));

			rollToNewFile();
			return true;
		}
		return false;
	}

	public WriteLogNode getLogNode() {
		return logNode;
	}

	public String getBufferwriteRestoreFilePath() {
		return bufferwriteRestoreFilePath;
	}
}
