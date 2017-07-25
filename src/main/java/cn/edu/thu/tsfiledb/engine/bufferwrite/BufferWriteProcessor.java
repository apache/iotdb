package cn.edu.thu.tsfiledb.engine.bufferwrite;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.conf.TSFileConfig;
import cn.edu.thu.tsfile.common.conf.TSFileDescriptor;
import cn.edu.thu.tsfile.common.constant.JsonFormatConstant;
import cn.edu.thu.tsfile.common.utils.BytesUtils;
import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.common.utils.RandomAccessOutputStream;
import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileWriter;
import cn.edu.thu.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.thu.tsfile.file.metadata.TSFileMetaData;
import cn.edu.thu.tsfile.file.metadata.TimeSeriesMetadata;
import cn.edu.thu.tsfile.file.metadata.converter.TSFileMetaDataConverter;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.thu.tsfile.format.FileMetaData;
import cn.edu.thu.tsfile.timeseries.write.TSRecordWriteSupport;
import cn.edu.thu.tsfile.timeseries.write.TSRecordWriter;
import cn.edu.thu.tsfile.timeseries.write.WriteSupport;
import cn.edu.thu.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.thu.tsfile.timeseries.write.record.DataPoint;
import cn.edu.thu.tsfile.timeseries.write.record.TSRecord;
import cn.edu.thu.tsfile.timeseries.write.schema.FileSchema;
import cn.edu.thu.tsfile.timeseries.write.series.IRowGroupWriter;
import cn.edu.thu.tsfiledb.conf.TsfileDBConfig;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.engine.exception.BufferWriteProcessorException;
import cn.edu.thu.tsfiledb.engine.lru.LRUProcessor;
import cn.edu.thu.tsfiledb.engine.utils.FlushState;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.metadata.ColumnSchema;
import cn.edu.thu.tsfiledb.metadata.MManager;
import cn.edu.thu.tsfiledb.sys.writelog.WriteLogManager;

public class BufferWriteProcessor extends LRUProcessor {

	private static final Logger LOGGER = LoggerFactory.getLogger(BufferWriteProcessor.class);
	private static final TSFileConfig TsFileConf = TSFileDescriptor.getInstance().getConfig();
	private static final TsfileDBConfig TsFileDBConf = TsfileDBDescriptor.getInstance().getConfig();
	private static final MManager mManager = MManager.getInstance();

	private boolean isFlushingSync = false;
	private final FlushState flushState = new FlushState();
	private ReadWriteLock convertBufferLock = new ReentrantReadWriteLock(false);

	private FileSchema fileSchema;
	private BufferWriteIOWriter bufferIOWriter;
	private BufferWriteRecordWriter recordWriter;
	private int lastRowgroupSize = 0;

	// this just the bufferwrite file name
	private String fileName;
	private static final String restoreFile = ".restore";
	// this is the bufferwrite file absolute path
	private String bufferwriteRestoreFilePath;
	private String bufferwriteOutputFilePath;

	private boolean isNewProcessor = false;

	private Action bufferwriteFlushAction = null;
	private Action bufferwriteCloseAction = null;
	private Action filenodeFlushAction = null;

	public BufferWriteProcessor(String nameSpacePath, String fileName, Map<String, Object> parameters)
			throws BufferWriteProcessorException {
		super(nameSpacePath);

		this.fileName = fileName;
		String restoreFileName = fileName + restoreFile;

		String bufferwriteDirPath = TsFileDBConf.bufferWriteDir;
		if (bufferwriteDirPath.length() > 0
				&& bufferwriteDirPath.charAt(bufferwriteDirPath.length() - 1) != File.separatorChar) {
			bufferwriteDirPath = bufferwriteDirPath + File.separatorChar;
		}
		String dataDirPath = bufferwriteDirPath + nameSpacePath;
		File dataDir = new File(dataDirPath);
		if (!dataDir.exists()) {
			dataDir.mkdirs();
			LOGGER.warn("The bufferwrite processor data dir doesn't exists, and mkdir the dir {}", dataDirPath);
		}
		File outputFile = new File(dataDir, fileName);
		File restoreFile = new File(dataDir, restoreFileName);
		bufferwriteRestoreFilePath = restoreFile.getAbsolutePath();
		bufferwriteOutputFilePath = outputFile.getAbsolutePath();
		// get the fileschema
		try {
			fileSchema = constructFileSchema(nameSpacePath);
		} catch (PathErrorException | WriteProcessException e) {
			LOGGER.error("Get the FileSchema error, the nameSpacePath is {}", nameSpacePath);
			throw new BufferWriteProcessorException(
					String.format("Get the FileSchema error, the nameSpacePath is %s, the reason is %s", nameSpacePath,
							e.getMessage()));
		}
		//
		// There is one damaged file, and the restoreFile exist
		//
		if (outputFile.exists() && restoreFile.exists()) {

			bufferwriteRecovery();

		} else {

			TSRandomAccessFileWriter outputWriter;
			try {
				outputWriter = new RandomAccessOutputStream(outputFile);
			} catch (IOException e) {
				LOGGER.error("Construct the TSRandomAccessFileWriter error, the absolutePath is {}",
						outputFile.getAbsolutePath());
				throw new BufferWriteProcessorException(
						String.format("Can't get the TSRandomAccessFileWriter, the abslutePath is %s, the reason is %s",
								outputFile.getAbsolutePath(), e.getMessage()));
			}

			try {
				bufferIOWriter = new BufferWriteIOWriter(fileSchema, outputWriter);
			} catch (IOException e) {
				LOGGER.error("Get the BufferWriteIOWriter error, the nameSpacePath is {}", nameSpacePath);
				throw new BufferWriteProcessorException(
						String.format("Get the BufferWriteIOWriter error, the nameSpacePath is %s, the reason is %s",
								nameSpacePath, e.getMessage()));
			}

			WriteSupport<TSRecord> writeSupport = new TSRecordWriteSupport();
			recordWriter = new BufferWriteRecordWriter(TsFileConf, bufferIOWriter, writeSupport, fileSchema);
			isNewProcessor = true;
			// write restore file
			writeStoreToDisk();
		}
		// init action
		// the action from the corresponding filenode processor
		bufferwriteFlushAction = (Action) parameters.get(FileNodeConstants.BUFFERWRITE_FLUSH_ACTION);
		bufferwriteCloseAction = (Action) parameters.get(FileNodeConstants.BUFFERWRITE_CLOSE_ACTION);
		filenodeFlushAction = (Action) parameters.get(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION);
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
			pair = ReadStoreFromDisk();
		} catch (IOException e) {
			LOGGER.error("Read restore file failed, reason is {}", e.getMessage());
			e.printStackTrace();
			throw new BufferWriteProcessorException("Read restore file failed, reason is " + e.getMessage());
		}
		TSRandomAccessFileWriter output;
		long lastPosition = pair.left;
		File lastBufferWriteFile = new File(bufferwriteOutputFilePath);
		if (lastBufferWriteFile.length() != lastPosition) {
			LOGGER.warn("The length of the last bufferwrite file is {}, the lastPosion is {}",
					lastBufferWriteFile.length(), lastPosition);
			try {
				cutOffFile(lastPosition);
			} catch (IOException e) {
				LOGGER.error(
						"Cut off damaged file error. the damaged file path is {}, the length is {}, the cut off length is {}",
						bufferwriteOutputFilePath, lastBufferWriteFile.length(), lastPosition);
				e.printStackTrace();
				throw new BufferWriteProcessorException(String.format(
						"Cut off damaged file error. the damaged file path is %s, the length is %s, the cut off length is %s",
						bufferwriteOutputFilePath, lastBufferWriteFile.length(), lastPosition));
			}
		}
		try {
			// Notice: the offset is seek to end of the file by API of kr
			output = new RandomAccessOutputStream(lastBufferWriteFile);
		} catch (IOException e) {
			LOGGER.error("Can't construct the RandomAccessOutputStream, the outputPath is {}",
					bufferwriteOutputFilePath);
			e.printStackTrace();
			throw new BufferWriteProcessorException(
					"Can't construct the RandomAccessOutputStream, the reason is " + e.getMessage());
		}
		try {
			// Notice: the parameter of lastPosition is not used beacuse of the
			// API of kr
			bufferIOWriter = new BufferWriteIOWriter(fileSchema, output, lastPosition, pair.right);
		} catch (IOException e) {
			LOGGER.error("Can't get the bufferwrite io when recovery, the nameSpacePath is {}.", nameSpacePath);
			e.printStackTrace();
			throw new BufferWriteProcessorException(String.format(
					"Can't get the bufferwrite io when recovery, the nameSpacePath is %s, the reason is %s",
					nameSpacePath, e.getMessage()));
		}
		WriteSupport<TSRecord> writeSupport = new TSRecordWriteSupport();
		recordWriter = new BufferWriteRecordWriter(TsFileConf, bufferIOWriter, writeSupport, fileSchema);
		isNewProcessor = false;
	}

	private void cutOffFile(long length) throws IOException {

		String tempPath = bufferwriteOutputFilePath + ".backup";
		File tempFile = new File(tempPath);
		File normalFile = new File(bufferwriteOutputFilePath);

		if (normalFile.exists() && normalFile.length() > 0) {

			RandomAccessFile normalReader = null;
			RandomAccessFile tempWriter = null;
			try {
				normalReader = new RandomAccessFile(normalFile, "r");
				tempWriter = new RandomAccessFile(tempFile, "rw");
			} catch (FileNotFoundException e) {
				LOGGER.error(
						"Can't get the RandomAccessFile read and write, the normal path is {}, the temp path is {}",
						bufferwriteOutputFilePath, tempPath);
				e.printStackTrace();
				if (normalReader != null) {
					normalReader.close();
				}
				if (tempWriter != null) {
					tempWriter.close();
				}
				throw e;
			}

			if (tempFile.exists()) {
				tempFile.delete();
			}
			long offset = 0;
			int step = 4 * 1024 * 1024;
			byte[] buff = new byte[step];
			while (length - offset >= step) {
				try {
					normalReader.readFully(buff);
					tempWriter.write(buff);
				} catch (IOException e) {
					LOGGER.error("normalReader read data failed or tempWriter write data error");
					e.printStackTrace();
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
			LOGGER.error("Can't get the bufferwrite io position");
			e.printStackTrace();
			throw new BufferWriteProcessorException(e);
		}
		List<RowGroupMetaData> rowGroupMetaDatas = bufferIOWriter.getRowGroups();
		List<RowGroupMetaData> appendMetadata = new ArrayList<>();
		for(int i = lastRowgroupSize;i<rowGroupMetaDatas.size();i++){
			appendMetadata.add(rowGroupMetaDatas.get(i));
		}
		lastRowgroupSize = rowGroupMetaDatas.size();
		// construct the tsfile metadate
		// List<TimeSeriesMetadata> timeSeriesList = fileSchema.getTimeSeriesMetadatas();
		List<TimeSeriesMetadata> timeSeriesList = new ArrayList<>();
		TSFileMetaData tsfileMetadata = new TSFileMetaData(appendMetadata, timeSeriesList,
				TSFileDescriptor.getInstance().getConfig().currentVersion);

		TSFileMetaDataConverter metadataConverter = new TSFileMetaDataConverter();
		RandomAccessFile out = null;
		try {
			out = new RandomAccessFile(bufferwriteRestoreFilePath,"rw");
		} catch (FileNotFoundException e) {
			LOGGER.error("The restore file can't be created, the file path is {}", bufferwriteRestoreFilePath);
			e.printStackTrace();
			throw new BufferWriteProcessorException(e);
		}
		try {
			// seek 8 byte
			if(out.length()>0){
				out.seek(out.length()-8);
			}
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ReadWriteThriftFormatUtils.writeFileMetaData(metadataConverter.toThriftFileMetadata(tsfileMetadata), baos);
			// write metadata size using int
			int metadataSize = baos.size();
			out.write(BytesUtils.intToBytes(metadataSize));
			// write metadata
			out.write(baos.toByteArray());
			// write tsfile position
			byte[] lastPositionBytes = BytesUtils.longToBytes(lastPosition);
			out.write(lastPositionBytes);
			LOGGER.info("Write restore information to the restore file");
		} catch (IOException e) {
			LOGGER.error("Serialize the TSFileMetaData error");
			e.printStackTrace();
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
	 * The left of the pair is the last position. The right of the pair is the
	 * rowGroupMetadata.
	 *
	 * @return - the left is the end position of the last rowgroup flushed, the
	 *         right is all the rowgroup meatdata flushed
	 * @throws IOException
	 */
	private Pair<Long, List<RowGroupMetaData>> ReadStoreFromDisk() throws IOException {
		byte[] lastPostionBytes = new byte[8];
		List<RowGroupMetaData> groupMetaDatas = new ArrayList<>();
		RandomAccessFile randomAccessFile = null;
		try {
			randomAccessFile = new RandomAccessFile(bufferwriteRestoreFilePath, "rw");
			long fileLength = randomAccessFile.length();
			// read tsfile position
			long point = randomAccessFile.getFilePointer();
			while(point+8<fileLength){
				byte[] metadataSizeBytes = new byte[4];
				randomAccessFile.read(metadataSizeBytes);
				int metadataSize = BytesUtils.bytesToInt(metadataSizeBytes);
				byte[] thriftBytes = new byte[metadataSize];
				randomAccessFile.read(thriftBytes);
				ByteArrayInputStream inputStream = new ByteArrayInputStream(thriftBytes);
				FileMetaData fileMetaData = ReadWriteThriftFormatUtils.readFileMetaData(inputStream);
				TSFileMetaDataConverter metadataConverter = new TSFileMetaDataConverter();
				TSFileMetaData tsFileMetaData = metadataConverter.toTSFileMetadata(fileMetaData);
				groupMetaDatas.addAll(tsFileMetaData.getRowGroups());
				lastRowgroupSize = groupMetaDatas.size();
				point = randomAccessFile.getFilePointer();
			}
			randomAccessFile.read(lastPostionBytes);
		} catch (FileNotFoundException e) {
			LOGGER.error("The restore file is not exist, the restore file path is {}", bufferwriteRestoreFilePath);
			e.printStackTrace();
			throw e;
		} catch (IOException e) {
			e.printStackTrace();
			LOGGER.error("read data from file error, the reasion is {}", e.getMessage());
			throw e;
		} finally {
			if(randomAccessFile!=null){
				randomAccessFile.close();
			}
		}
		long lastPostion = BytesUtils.bytesToLong(lastPostionBytes);
		Pair<Long, List<RowGroupMetaData>> result = new Pair<Long, List<RowGroupMetaData>>(lastPostion, groupMetaDatas);
		return result;
	}

	private FileSchema constructFileSchema(String nameSpacePath) throws PathErrorException, WriteProcessException {
		List<ColumnSchema> columnSchemaList;

		String deltaObjectType = null;
		try {
			deltaObjectType = mManager.getDeltaObjectTypeByPath(nameSpacePath);
		} catch (PathErrorException e) {
			LOGGER.error("Get the deltaObjectType from MManager error using nameSpacePath is {}", nameSpacePath);
			throw e;
		}
		try {
			columnSchemaList = mManager.getSchemaForOneType(deltaObjectType);
		} catch (PathErrorException e) {
			LOGGER.error("The list of ColumnSchema error from MManager error using deltaObjectType is {}",
					deltaObjectType);
			throw e;
		}
		FileSchema fileSchema = null;
		try {
			fileSchema = getFileSchemaFromColumnSchema(columnSchemaList, deltaObjectType);
		} catch (WriteProcessException e) {
			LOGGER.error("Get the FileSchema error, the list of ColumnSchema is {}", columnSchemaList);
			throw e;
		}
		return fileSchema;

	}

	private FileSchema getFileSchemaFromColumnSchema(List<ColumnSchema> schemaList, String deltaObjectType)
			throws WriteProcessException {
		JSONArray rowGroup = new JSONArray();

		for (ColumnSchema col : schemaList) {
			JSONObject measurement = new JSONObject();
			measurement.put(JsonFormatConstant.MEASUREMENT_UID, col.name);
			measurement.put(JsonFormatConstant.DATA_TYPE, col.dataType.toString());
			measurement.put(JsonFormatConstant.MEASUREMENT_ENCODING, col.encoding.toString());
			for (Entry<String, String> entry : col.getArgsMap().entrySet()) {
				if (JsonFormatConstant.ENUM_VALUES.equals(entry.getKey())) {
					String[] valueArray = entry.getValue().split(",");
					measurement.put(JsonFormatConstant.ENUM_VALUES, new JSONArray(valueArray));
				} else
					measurement.put(entry.getKey(), entry.getValue().toString());
			}
			rowGroup.put(measurement);
		}
		JSONObject jsonSchema = new JSONObject();
		jsonSchema.put(JsonFormatConstant.JSON_SCHEMA, rowGroup);
		jsonSchema.put(JsonFormatConstant.DELTA_TYPE, deltaObjectType);
		return new FileSchema(jsonSchema);
	}

	public String getFileName() {
		return fileName;
	}

	public String getFileAbsolutePath() {
		return bufferwriteOutputFilePath;
	}

	public boolean isNewProcessor() {
		return isNewProcessor;
	}

	public void setNewProcessor(boolean isNewProcessor) {
		this.isNewProcessor = isNewProcessor;
	}

	/**
	 * Write a data point
	 *
	 * @param deltaObjectId
	 * @param measurementId
	 * @param timestamp
	 * @param dataType
	 * @param value
	 * @throws BufferWriteProcessorException
	 * @throws IOException
	 */
	public void write(String deltaObjectId, String measurementId, long timestamp, TSDataType dataType, String value)
			throws BufferWriteProcessorException {
		TSRecord record = new TSRecord(timestamp, deltaObjectId);
		DataPoint dataPoint = DataPoint.getDataPoint(dataType, measurementId, value);
		record.addTuple(dataPoint);
		write(record);
	}

	/**
	 * Write a tsRecord
	 *
	 * @param tsRecord
	 * @throws BufferWriteProcessorException
	 */
	public void write(TSRecord tsRecord) throws BufferWriteProcessorException {

		try {
			recordWriter.write(tsRecord);
		} catch (IOException | WriteProcessException e) {
			LOGGER.error("Write TSRecord error, the TSRecord is {}, the nameSpacePath is {}", tsRecord, nameSpacePath);
			throw new BufferWriteProcessorException(
					String.format("Write TSRecord error, the TSRecord is %s, the nameSpacePath is %s, the reason is %s",
							tsRecord, nameSpacePath, e.getMessage()));
		}
	}

	public Pair<List<Object>, List<RowGroupMetaData>> getIndexAndRowGroupList(String deltaObjectId,
			String measurementId) {
		List<Object> memData = null;
		List<RowGroupMetaData> list = null;
		// wait until flush over
		synchronized (flushState) {
			while (flushState.isFlushing()) {
				try {
					flushState.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
					LOGGER.error(e.getMessage());
				}
			}
		}
		convertBufferLock.readLock().lock();
		try {
			memData = recordWriter.query(deltaObjectId, measurementId);
			list = bufferIOWriter.getCurrentRowGroupMetaList(deltaObjectId);
		} finally {
			convertBufferLock.readLock().unlock();
		}
		return new Pair<>(memData, list);
	}

	@Override
	public boolean canBeClosed() {
		LOGGER.info("Check nameSpacePath {} can be closed or not", nameSpacePath);
		if (flushState.isFlushing()) {
			LOGGER.info("The nameSpacePath {} can't be closed", nameSpacePath);
			return false;
		} else {
			LOGGER.info("The nameSpacePath {} can be closed", nameSpacePath);
			return true;
		}
	}

	@Override
	public void close() throws BufferWriteProcessorException {
		isFlushingSync = true;
		try {
			recordWriter.close();
			// update the intervalfile for interval list
			bufferwriteCloseAction.act();
			// flush the changed information for filenode
			filenodeFlushAction.act();
			// delete the restore for this bufferwrite processor
			deleteRestoreFile();
		} catch (IOException e) {
			LOGGER.error("Close the bufferwrite processor error, the nameSpacePath is {}", nameSpacePath);
			throw new BufferWriteProcessorException(
					String.format("Close the bufferwrite processor error, the nameSpacePath is %s, the reason is %s",
							nameSpacePath, e.getMessage()));
		} catch (Exception e) {
			LOGGER.error("Close the bufferwrite processor failed, when call the action function. The reason is {}",
					e.getMessage());
			e.printStackTrace();
			throw new BufferWriteProcessorException(
					"Close the bufferwrite processor failed, when call the action function");
		} finally {
			isFlushingSync = false;
		}

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
		 * @throws Exception
		 * @throws WriteProcessException
		 * @throws IOException
		 */
		@Override
		public void write(TSRecord record) throws IOException, WriteProcessException {
			try {
				super.write(record);
			} catch (IOException | WriteProcessException e) {
				LOGGER.error("Write TSRecord error, TSRecord is {}", record);
				throw e;
			}
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
							LOGGER.error("Interrupt error when waiting flush,processor:{}", nameSpacePath, e);
						}
					}
				}
				// update the lastUpdatetime
				try {
					bufferwriteFlushAction.act();
				} catch (Exception e) {
					LOGGER.error("Flush bufferwrite row group failed, when call the action function.", e);
					throw new IOException(e);
				}

				if (TsfileDBDescriptor.getInstance().getConfig().enableWal) {
					// For WAL
					WriteLogManager.getInstance().startBufferWriteFlush(nameSpacePath);
				}

				// flush bufferwrite data
				if (isFlushingSync) {
					try {
						super.flushRowGroup(false);
						writeStoreToDisk();
						filenodeFlushAction.act();
						if (TsfileDBDescriptor.getInstance().getConfig().enableWal) {
							WriteLogManager.getInstance().endBufferWriteFlush(nameSpacePath);
						}
					} catch (IOException e) {
						LOGGER.error("Flush row group to store failed, processor:{}.", nameSpacePath, e);
						throw e;
					} catch (BufferWriteProcessorException e) {
						// write restore error
						LOGGER.error("Write bufferwrite information to disk failed", e);
						throw new IOException("Write bufferwrite information to disk failed");
					} catch (Exception e) {
						// action error
						LOGGER.error("Flush bufferwrite row group failed, when call the action function", e);
						// handle
						throw new IOException("Flush bufferwrite row group failed, when call the action function");
					}
				} else {
					flushState.setFlushing();
					switchIndexFromWorkToFlush();
					switchRecordWriterFromWorkToFlush();

					Runnable flushThread;
					flushThread = () -> {
						LOGGER.info("Asynchronous flushing start,-Thread id {}", Thread.currentThread().getId());
						try {
							asyncFlushRowGroupToStore();
							writeStoreToDisk();
							filenodeFlushAction.act();
							if (TsfileDBDescriptor.getInstance().getConfig().enableWal) {
								WriteLogManager.getInstance().endBufferWriteFlush(nameSpacePath);
							}
						} catch (IOException e) {
							/*
							 * There should be added system log by CGF and throw
							 * exception
							 */
							LOGGER.error("{} Asynchronous flush error, sleep this thread-{}.", nameSpacePath,
									Thread.currentThread().getId(), e);
						} catch (BufferWriteProcessorException e) {
							LOGGER.error("Write bufferwrite information to disk failed.", e);
							// how to handle this error
							// TODO
						} catch (Exception e) {
							// action error
							LOGGER.error("Flush bufferwrite row group failed, when call the action function.", e);
							// how to handle this error
							// TODO
						}
						switchRecordWriterFromFlushToWork();
						convertBufferLock.writeLock().lock();
						try {
							synchronized (flushState) {
								switchIndexFromFlushToWork();
								LOGGER.info("Asynchronous flushing end,-Thread is {}", Thread.currentThread().getId());
								flushState.setUnFlushing();
								flushState.notify();
							}
						} finally {
							convertBufferLock.writeLock().unlock();
						}
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
				// remove the feature: fill the row group
				// fillInRowGroupSize(actualTotalRowGroupSize);
				LOGGER.info("Asynchronous total row group size:{}, actual:{}, less:{}", primaryRowGroupSize,
						actualTotalRowGroupSize, primaryRowGroupSize - actualTotalRowGroupSize);
				LOGGER.info("Asynchronous write row group end");
			}
		}

		private void switchRecordWriterFromWorkToFlush() {

			flushingRowGroupWriters = groupWriters;
			flushingRowGroupSet = new HashSet<>();
			for (String DeltaObjectId : schema.getDeltaObjectAppearedSet()) {
				flushingRowGroupSet.add(DeltaObjectId);
			}
			flushingRecordCount = recordCount;
			// reset
			groupWriters = new HashMap<String, IRowGroupWriter>();
			writeSupport.init(groupWriters);
			schema.getDeltaObjectAppearedSet().clear();
			recordCount = 0;
		}

		private void switchRecordWriterFromFlushToWork() {
			flushingRowGroupSet = null;
			flushingRowGroupWriters = null;
			flushingRecordCount = -1;
		}
	}

	private void switchIndexFromWorkToFlush() {

	}

	private void switchIndexFromFlushToWork() {
		bufferIOWriter.addNewRowGroupMetaDataToBackUp();
	}
}
