package cn.edu.tsinghua.iotdb.engine.bufferwriteV2;

import java.io.File;
import java.io.IOException;
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
import cn.edu.tsinghua.iotdb.engine.bufferwrite.Action;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.FileNodeConstants;
import cn.edu.tsinghua.iotdb.engine.memcontrol.BasicMemController;
import cn.edu.tsinghua.iotdb.engine.memtable.IMemTable;
import cn.edu.tsinghua.iotdb.engine.memtable.MemSeriesLazyMerger;
import cn.edu.tsinghua.iotdb.engine.memtable.PrimitiveMemTable;
import cn.edu.tsinghua.iotdb.engine.pool.FlushManager;
import cn.edu.tsinghua.iotdb.engine.querycontext.RawSeriesChunk;
import cn.edu.tsinghua.iotdb.engine.querycontext.RawSeriesChunkLazyLoadImpl;
import cn.edu.tsinghua.iotdb.engine.utils.FlushStatus;
import cn.edu.tsinghua.iotdb.exception.BufferWriteProcessorException;
import cn.edu.tsinghua.iotdb.utils.MemUtils;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;

public class BufferWriteProcessor extends Processor {
	private static final Logger LOGGER = LoggerFactory.getLogger(BufferWriteProcessor.class);

	private FileSchema fileSchema;
	private BufferWriteResource bufferWriteResource;

	private volatile FlushStatus flushStatus = new FlushStatus();
	private volatile boolean isFlush;
	private ReentrantLock flushQueryLock = new ReentrantLock();
	private AtomicLong memSize = new AtomicLong();
	private long memThreshold = TSFileDescriptor.getInstance().getConfig().groupSizeInByte;

	private IMemTable workMemTable;
	private IMemTable flushMemTable;

	private Action bufferwriteFlushAction = null;
	private Action bufferwriteCloseAction = null;
	private Action filenodeFlushAction = null;

	private long lastFlushTime = -1;
	private long valueCount = 0;

	private String fileName;
	private String insertFilePath;

	private WriteLogNode logNode;

	public BufferWriteProcessor(String processorName, String fileName, Map<String, Object> parameters,
			FileSchema fileSchema) throws BufferWriteProcessorException {
		super(processorName);
		this.fileSchema = fileSchema;
		this.fileName = fileName;
		String bufferwriteDirPath = TsfileDBDescriptor.getInstance().getConfig().bufferWriteDir;
		if (bufferwriteDirPath.length() > 0
				&& bufferwriteDirPath.charAt(bufferwriteDirPath.length() - 1) != File.separatorChar) {
			bufferwriteDirPath = bufferwriteDirPath + File.separatorChar;
		}
		String dataDirPath = bufferwriteDirPath + processorName;
		File dataDir = new File(dataDirPath);
		if (!dataDir.exists()) {
			dataDir.mkdirs();
			LOGGER.debug("The bufferwrite processor data dir doesn't exists, create new directory {}.", dataDirPath);
		}
		this.insertFilePath = new File(dataDir, fileName).getPath();
		try {
			bufferWriteResource = new BufferWriteResource(processorName, insertFilePath);
		} catch (IOException e) {
			throw new BufferWriteProcessorException(e);
		}
		bufferwriteFlushAction = (Action) parameters.get(FileNodeConstants.BUFFERWRITE_FLUSH_ACTION);
		bufferwriteCloseAction = (Action) parameters.get(FileNodeConstants.BUFFERWRITE_CLOSE_ACTION);
		filenodeFlushAction = (Action) parameters.get(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION);
		workMemTable = new PrimitiveMemTable();

		if(TsfileDBDescriptor.getInstance().getConfig().enableWal) {
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
					bufferWriteResource.getInsertMetadatas(deltaObjectId, measurementId, dataType));
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

	private void swithFlushToWork() {
		flushQueryLock.lock();
		try {
			flushMemTable.clear();
			flushMemTable = null;
			bufferWriteResource.appendMetadata();
		} finally {
			isFlush = false;
			flushQueryLock.unlock();
		}
	}

	private void flushOperation(String flushFunction) {
		long flushStartTime = System.currentTimeMillis();
		LOGGER.info("The bufferwrite processor {} starts flushing {}.", getProcessorName(), flushFunction);
		try {
			bufferWriteResource.flush(fileSchema, flushMemTable);
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
				swithFlushToWork();
				flushStatus.notify();
				LOGGER.info("The bufferwrite processor {} ends flushing {}.", getProcessorName(), flushFunction);
			}
		}
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
				LOGGER.error("Failed to flush bufferwrite row group when calling the action function.");
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
		return true;
	}

	@Override
	public void close() throws ProcessorException {
		try {
			long closeStartTime = System.currentTimeMillis();
			// flush data
			flush(true);
			// end file
			bufferWriteResource.close(fileSchema);
			// update the intervalfile for interval list
			bufferwriteCloseAction.act();
			// flush the changed information for filenode
			filenodeFlushAction.act();
			// delete the restore for this bufferwrite processor
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
		// TODO : save this variable to avoid object creation?
		File file = new File(insertFilePath);
		return file.length() + memoryUsage();
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

	private String getBufferwriteRestoreFilePath() {
		return bufferWriteResource.getRestoreFilePath();
	}

}
