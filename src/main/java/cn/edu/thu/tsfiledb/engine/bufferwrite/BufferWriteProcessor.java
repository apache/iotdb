package cn.edu.thu.tsfiledb.engine.bufferwrite;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.conf.TSFileConfig;
import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfile.timeseries.write.TSRecordWriter;
import cn.edu.thu.tsfile.timeseries.write.WriteSupport;
import cn.edu.thu.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.thu.tsfile.timeseries.write.record.DataPoint;
import cn.edu.thu.tsfile.timeseries.write.record.TSRecord;
import cn.edu.thu.tsfile.timeseries.write.schema.FileSchema;
import cn.edu.thu.tsfile.timeseries.write.series.IRowGroupWriter;
import cn.edu.thu.tsfiledb.engine.utils.FlushState;

/**
 * BufferWriteProcessor is used to write one data point or one tsRecord whose
 * timestamp is greater than than the last update time in
 * {@code FileNodeProcessor}<br>
 *
 * @author kangrong
 * @author liukun
 */
public class BufferWriteProcessor extends LRUProcessor {
	private static final Logger LOG = LoggerFactory.getLogger(BufferWriteProcessor.class);

	private BufferWriteIndex workingBufferIndex;
	private BufferWriteIndex flushingBufferIndex;

	private boolean isFlushingSync = false;
	private FlushState flushState = new FlushState();

	private BufferWriteIOWriter bufferIOWriter;
	private ReadWriteLock convertBufferLock = new ReentrantReadWriteLock(false);
	private BufferWriteRecordWriter recordWriter = null;
	private Pair<String, String> fileNamePair;
	private boolean isNewProcessor = false;

	private Action closeAction;

	BufferWriteProcessor(TSFileConfig conf, BufferWriteIOWriter ioFileWriter, WriteSupport<TSRecord> writeSupport,
			FileSchema schema, String nsPath) {
		super(nsPath);
		this.workingBufferIndex = new MemoryBufferWriteIndexImpl();
		this.flushingBufferIndex = new MemoryBufferWriteIndexImpl();
		this.bufferIOWriter = ioFileWriter;
		this.recordWriter = new BufferWriteRecordWriter(conf, ioFileWriter, writeSupport, schema);
	}

	BufferWriteProcessor(TSFileConfig conf, BufferWriteIOWriter ioFileWriter, WriteSupport<TSRecord> writeSupport,
			FileSchema schema, String nsPath, Pair<String, String> fileNamePair) {
		super(nsPath);
		this.workingBufferIndex = new MemoryBufferWriteIndexImpl();
		this.flushingBufferIndex = new MemoryBufferWriteIndexImpl();
		this.bufferIOWriter = ioFileWriter;
		this.recordWriter = new BufferWriteRecordWriter(conf, ioFileWriter, writeSupport, schema);
		this.fileNamePair = fileNamePair;
		this.isNewProcessor = true;
	}

	/**
	 * Write a data point
	 * 
	 * @param deltaObjectId
	 * @param measurementId
	 * @param timestamp
	 * @param deltaObjectType
	 * @param dataType
	 * @param value
	 * @throws WriteProcessException
	 * @throws IOException
	 */
	public void write(String deltaObjectId, String measurementId, long timestamp, String deltaObjectType,
			TSDataType dataType, String value) throws IOException, WriteProcessException {
		TSRecord record = new TSRecord(timestamp, deltaObjectId);
		DataPoint dataPoint = DataPoint.getDataPoint(dataType, measurementId, value);
		record.addTuple(dataPoint);
		write(record);
	}

	/**
	 * Write a tsRecord
	 * 
	 * @param tsRecord
	 * @throws WriteProcessException
	 * @throws IOException
	 */
	public void write(TSRecord tsRecord) throws IOException, WriteProcessException {
		workingBufferIndex.insert(tsRecord);
		recordWriter.write(tsRecord);
	}

	private void switchIndexFromWorkingToFlushing() {
		BufferWriteIndex temp = workingBufferIndex;
		workingBufferIndex = flushingBufferIndex;
		flushingBufferIndex = temp;

	}

	private void switchIndexFromFlushingToWorking() {
		flushingBufferIndex.clear();
		bufferIOWriter.addNewRowGroupMetaDataToBackUp();
		flushState.setUnFlushing();
	}

	/**
	 * Get the result of DynamicOneColumnData from work index and flush index
	 * 
	 * @param deltaObjectId
	 * @param measurementId
	 * @return
	 */
	private DynamicOneColumnData mergeTwoDynamicColumnData(String deltaObjectId, String measurementId) {
		DynamicOneColumnData working = workingBufferIndex.query(deltaObjectId, measurementId);
		DynamicOneColumnData ret = (working == null || working.length == 0) ? new DynamicOneColumnData()
				: new DynamicOneColumnData(working.dataType, true);
		if (flushState.isFlushing()) {
			DynamicOneColumnData flushing = flushingBufferIndex.query(deltaObjectId, measurementId);
			ret.mergeRecord(flushing);
		}
		ret.mergeRecord(working);
		return ret;
	}

	@Override
	public void close() {
		isFlushingSync = true;
		try {
			recordWriter.close();
			isFlushingSync = false;
		} catch (IOException e) {
			LOG.error("Close BufferWriteProcessor error in namespace {}. Message:{}", nameSpacePath, e.getMessage());
			e.printStackTrace();
		}
		closeAction.act();

		/*
		 * try { WriteLogManager.getInstance().bufferFlush(); } catch (Exception
		 * e) { LOG.error("Write log for bufferredWrite error", e);
		 * System.exit(1); }
		 */
	}

	public void setCloseAction(Action action) {
		this.closeAction = action;
	}

	public Action getCloseAction() {
		return this.closeAction;
	}

	@Override
	public boolean canBeClosed() {
		LOG.debug("{} can be closed-{}", nameSpacePath, !flushState.isFlushing());
		return !flushState.isFlushing();
	}

	public Pair<DynamicOneColumnData, List<RowGroupMetaData>> getIndexAndRowGroupList(String deltaObjectId,
			String measurementId) {
		convertBufferLock.readLock().lock();
		DynamicOneColumnData index = null;
		List<RowGroupMetaData> list = null;
		try {
			index = mergeTwoDynamicColumnData(deltaObjectId, measurementId);
			list = bufferIOWriter.getCurrentRowGroupMetaList();
		} finally {
			convertBufferLock.readLock().unlock();
		}
		return new Pair<>(index, list);
	}

	public Pair<String, String> getPair() {
		return fileNamePair;
	}

	public void setIsNewProcessor(boolean isNewProcessor) {
		this.isNewProcessor = isNewProcessor;
	}

	public boolean isNewProcessor() {
		return isNewProcessor;
	}

	private class BufferWriteRecordWriter extends TSRecordWriter {

		private Map<String, IRowGroupWriter> flushingRowGroupWriters;
		private Set<String> flushingRowGroupSet;
		private long flushingRecordCount;

		BufferWriteRecordWriter(TSFileConfig conf, BufferWriteIOWriter ioFileWriter,
				WriteSupport<TSRecord> writeSupport, FileSchema schema) {
			super(conf, ioFileWriter, writeSupport, schema);
		}

		/**
		 * insert a list of data value in form of TimePair.
		 *
		 * @param record
		 *            - TSRecord to be written
		 * @throws WriteProcessException
		 * @throws IOException
		 */
		@Override
		public void write(TSRecord record) throws IOException, WriteProcessException {
			super.write(record);
		}

		@Override
		protected void flushRowGroup(boolean isFillRowGroup) throws IOException {
			if (recordCount > 0) {
				synchronized (flushState) {
					// This thread wait until the subThread flush finished
					while (flushState.isFlushing()) {
						try {
							flushState.wait();
						} catch (InterruptedException e) {
							LOG.error("Interrupt error when waiting flush,processor:{}. Message: {}", nameSpacePath,
									e.getMessage());
							e.printStackTrace();
						}
					}
				}
				if (isFlushingSync) {
					try {
						super.flushRowGroup(false);
					} catch (IOException e) {
						LOG.error("Flush row group to store failed, processor:{}. Message: {}", nameSpacePath,
								e.getMessage());
						/*
						 * There should be added system log by CGF and throw
						 * exception
						 */
						e.printStackTrace();
						System.exit(1);
					}
				} else {
					flushState.setFlushing();
					switchIndexFromWorkingToFlushing();
					switchRecordWriterFromWorkingToFlushing();
					Runnable flushThread;
					flushThread = () -> {
						LOG.info("Asynchronous flushing start,-Thread id {}", Thread.currentThread().getId());
						try {
							asyncFlushRowGroupToStore();
						} catch (IOException e) {
							/*
							 * There should be added system log by CGF and throw
							 * exception
							 */
							LOG.error("{} Asynchronous flush error, sleep this thread-{}. Message:{}", nameSpacePath,
									Thread.currentThread().getId(), e.getMessage());
							e.printStackTrace();
							while (true) {
								try {
									Thread.sleep(3000);
								} catch (InterruptedException e1) {
									e1.printStackTrace();
								}
							}
						}
						switchRecordWriterFromFlushingToWorking();
						convertBufferLock.writeLock().lock();
						// notify the thread which is waiting for asynchronous
						// flush
						synchronized (flushState) {
							switchIndexFromFlushingToWorking();
							flushState.notify();
						}
						convertBufferLock.writeLock().unlock();
						LOG.info("Asynchronous flushing end,-Thread is {}", Thread.currentThread().getId());
					};
					Thread flush = new Thread(flushThread);
					flush.start();
				}
			}
		}

		private void asyncFlushRowGroupToStore() throws IOException {
			if (flushingRecordCount > 0) {
				String deltaObjectType = schema.getDeltaType();
				long totalMemStart = deltaFileWriter.getPos();
				for (String deltaObjectId : flushingRowGroupSet) {
					long rowGroupStart = deltaFileWriter.getPos();
					deltaFileWriter.startRowGroup(flushingRecordCount, deltaObjectId, deltaObjectType);
					IRowGroupWriter groupWriter = flushingRowGroupWriters.get(deltaObjectId);
					groupWriter.flushToFileWriter(deltaFileWriter);
					deltaFileWriter.endRowGroup(deltaFileWriter.getPos() - rowGroupStart);
				}
				long actualTotalRowGroupSize = deltaFileWriter.getPos() - totalMemStart;
				fillInRowGroupSize(actualTotalRowGroupSize);
				LOG.info("Asynchronous total row group size:{}, actual:{}, less:{}", primaryRowGroupSize,
						actualTotalRowGroupSize, primaryRowGroupSize - actualTotalRowGroupSize);
				LOG.info("Asynchronous write row group end");
			}
		}

		private void switchRecordWriterFromWorkingToFlushing() {

			flushingRowGroupWriters = groupWriters;
			flushingRowGroupSet = schema.getDeltaObjectAppearedSet();
			for (String DeltaObjectId : schema.getDeltaObjectAppearedSet()) {
				flushingRowGroupSet.add(DeltaObjectId);
			}
			flushingRecordCount = recordCount;
			// reset
			groupWriters = new HashMap<String, IRowGroupWriter>();
			schema.getDeltaObjectAppearedSet().clear();
			recordCount = 0;

		}

		private void switchRecordWriterFromFlushingToWorking() {
			flushingRowGroupSet = null;
			flushingRowGroupWriters = null;
			flushingRecordCount = -1;
		}
	}
}
