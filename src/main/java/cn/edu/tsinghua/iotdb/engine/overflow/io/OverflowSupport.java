package cn.edu.tsinghua.iotdb.engine.overflow.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.engine.overflow.metadata.OFFileMetadata;
import cn.edu.tsinghua.iotdb.engine.overflow.metadata.OFRowGroupListMetadata;
import cn.edu.tsinghua.iotdb.engine.overflow.metadata.OFSeriesListMetadata;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.compress.Compressor;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;

/**
 * Its main function is receiving a TSRecord with multiple data points. then
 * split the data points into corresponding series group.
 *
 * @author kangrong
 * @author liukun
 */
public class OverflowSupport {
	private static final Logger LOGGER = LoggerFactory.getLogger(OverflowSupport.class);
	private long lastFileOffset;

	private Map<String, Map<String, OverflowSeriesImpl>> overflowMap;
	private OverflowFileIO fileWriter;
	private Compressor compressor;

	private TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();

	@Deprecated
	public OverflowSupport(OverflowFileIO fileWriter) throws IOException {
		try {
			lastFileOffset = fileWriter.getTail();
		} catch (IOException e) {
			LOGGER.error("Get the tail of the file failed. reason: {}", e.getMessage());
			throw e;
		}
		this.fileWriter = fileWriter;
		try {
			init();
		} catch (IOException e) {
			LOGGER.error("Initialize the overflow support failed. reason:{}", e.getMessage());
			throw e;
		}
	}

	public OverflowSupport(OverflowFileIO fileWriter, OFFileMetadata ofFileMetadata) throws IOException {
		this.fileWriter = fileWriter;
		if (ofFileMetadata == null) {
			// recovery from file level
			try {
				lastFileOffset = fileWriter.getTail();
			} catch (IOException e) {
				LOGGER.error("Get the tail of the file failed. reason: {}", e.getMessage());
				throw e;
			}
		} else {
			// recovery from rowgroup level
			lastFileOffset = ofFileMetadata.getLastFooterOffset();
		}
		init(lastFileOffset, ofFileMetadata);
	}

	private void init(long lastFileOffset, OFFileMetadata ofFileMetadata) throws IOException {
		this.compressor = Compressor.getCompressor(conf.compressor);
		this.overflowMap = new HashMap<>();

		// overflow data from file for metaForread
		Map<String, Map<String, List<TimeSeriesChunkMetaData>>> metaForReader = fileWriter
				.getSeriesListMap(lastFileOffset);
		for (Entry<String, Map<String, List<TimeSeriesChunkMetaData>>> devEntry : metaForReader.entrySet()) {
			String deltaObjectId = devEntry.getKey();
			Map<String, OverflowSeriesImpl> seriesImplMap = new HashMap<>();
			for (Entry<String, List<TimeSeriesChunkMetaData>> senEntry : devEntry.getValue().entrySet()) {
				String measurementId = senEntry.getKey();
				List<TimeSeriesChunkMetaData> seriesList = senEntry.getValue();
				TimeSeriesChunkMetaData first = seriesList.get(0);
				Compressor compressor = Compressor.getCompressor(first.getProperties().getCompression());
				TSDataType type = first.getVInTimeSeriesChunkMetaData().getDataType();
				OverflowSeriesImpl seriesImpl = new OverflowSeriesImpl(measurementId, type, fileWriter, compressor,
						senEntry.getValue());
				seriesImplMap.put(measurementId, seriesImpl);
			}
			overflowMap.put(deltaObjectId, seriesImplMap);
		}
		// overflow data from rowgroup for metaForWriter
		if (ofFileMetadata != null) {
			for (OFRowGroupListMetadata rowGroupListMeta : ofFileMetadata.getMetaDatas()) {
				String deltaObjectId = rowGroupListMeta.getDeltaObjectId();
				if (!overflowMap.containsKey(deltaObjectId)) {
					overflowMap.put(deltaObjectId, new HashMap<>());
				}
				for (OFSeriesListMetadata seriesListMeta : rowGroupListMeta.getMetaDatas()) {
					String measurementId = seriesListMeta.getMeasurementId();
					if (!overflowMap.get(deltaObjectId).containsKey(measurementId)) {
						TimeSeriesChunkMetaData first = seriesListMeta.getMetaDatas().get(0);
						Compressor compressor = Compressor.getCompressor(first.getProperties().getCompression());
						TSDataType type = first.getVInTimeSeriesChunkMetaData().getDataType();
						OverflowSeriesImpl overflowSeriesImpl = new OverflowSeriesImpl(measurementId, type, fileWriter,
								compressor, null);
						overflowMap.get(deltaObjectId).put(measurementId, overflowSeriesImpl);
					}
					overflowMap.get(deltaObjectId).get(measurementId).setMetaForWriter(seriesListMeta.getMetaDatas());
				}
			}
		}
	}

	/**
	 * Initialize the overflow map from the overflow file
	 * 
	 * @deprecated
	 * @throws IOException
	 */
	private void init() throws IOException {
		this.compressor = Compressor.getCompressor(conf.compressor);
		this.overflowMap = new HashMap<>();
		Map<String, Map<String, List<TimeSeriesChunkMetaData>>> metaForReader = fileWriter.getSeriesListMap();
		for (Entry<String, Map<String, List<TimeSeriesChunkMetaData>>> devEntry : metaForReader.entrySet()) {
			String deltaObjectId = devEntry.getKey();
			Map<String, OverflowSeriesImpl> seriesImplMap = new HashMap<>();
			for (Entry<String, List<TimeSeriesChunkMetaData>> senEntry : devEntry.getValue().entrySet()) {
				String measurementId = senEntry.getKey();
				List<TimeSeriesChunkMetaData> seriesList = senEntry.getValue();
				TimeSeriesChunkMetaData first = seriesList.get(0);
				Compressor compressor = Compressor.getCompressor(first.getProperties().getCompression());
				TSDataType type = first.getVInTimeSeriesChunkMetaData().getDataType();
				OverflowSeriesImpl seriesImpl = new OverflowSeriesImpl(measurementId, type, fileWriter, compressor,
						senEntry.getValue());
				seriesImplMap.put(measurementId, seriesImpl);
			}
			overflowMap.put(deltaObjectId, seriesImplMap);
		}
	}

	/**
	 * Construct OverflowSeriesImpl for this deltaObjectId and measurementId, if
	 * not exist
	 *
	 * @param deltaObjectId
	 * @param measurementId
	 * @param type
	 * @return if create seriesImpl fail, return false
	 * @throws IOException
	 */
	private boolean checkRecord(String deltaObjectId, String measurementId, TSDataType type) {
		if (!overflowMap.containsKey(deltaObjectId))
			overflowMap.put(deltaObjectId, new HashMap<>());
		Map<String, OverflowSeriesImpl> deltaObjectIdWriterMap = overflowMap.get(deltaObjectId);
		OverflowSeriesImpl seriesWriter;
		if (!deltaObjectIdWriterMap.containsKey(measurementId)) {
			seriesWriter = new OverflowSeriesImpl(measurementId, type, fileWriter, compressor, null);
			deltaObjectIdWriterMap.put(measurementId, seriesWriter);
			return true;
		}
		seriesWriter = deltaObjectIdWriterMap.get(measurementId);
		if (type != seriesWriter.getTSDataType()) {
			return false;
		}
		return true;
	}

	/**
	 * Check the OverflowSeriesImpl is exist or not
	 * 
	 * @param deltaObjectId
	 * @param measurementId
	 * @return
	 */
	private boolean checkRecordForQuery(String deltaObjectId, String measurementId) {
		return overflowMap.containsKey(deltaObjectId) && overflowMap.get(deltaObjectId).containsKey(measurementId);
	}

	/**
	 * Calculate the all memory size for this overflow support
	 * 
	 * @return
	 */
	public long calculateMemSize() {
		long memSize = 0L;

		for (Map<String, OverflowSeriesImpl> measurementWriters : overflowMap.values()) {
			for (OverflowSeriesImpl measurementWrite : measurementWriters.values()) {
				memSize += measurementWrite.getMemorySize();
			}
		}
		return memSize;
	}

	/**
	 * Insert an overflow record.
	 *
	 * @param deltaObjectId
	 * @param measurementId
	 * @param type
	 * @param value
	 * @throws IOException
	 */
	public boolean insert(String deltaObjectId, String measurementId, long timestamp, TSDataType type, byte[] value) {
		if (!checkRecord(deltaObjectId, measurementId, type))
			return false;
		overflowMap.get(deltaObjectId).get(measurementId).insert(timestamp, value);
		return true;
	}

	/**
	 * Update all record to the parameter value in the range from start time to
	 * end time
	 *
	 * @param deltaObjectId
	 * @param measurementId
	 * @param startTime
	 * @param endTime
	 * @param value
	 */
	public boolean update(String deltaObjectId, String measurementId, long startTime, long endTime, TSDataType type,
			byte[] value) {
		if (!checkRecord(deltaObjectId, measurementId, type))
			return false;
		overflowMap.get(deltaObjectId).get(measurementId).update(startTime, endTime, value);
		return true;
	}

	/**
	 * Delete all value which timestamp is earlier than the parameter timestamp
	 *
	 * @param deltaObjectId
	 * @param measurementId
	 * @param timestamp
	 * @param type
	 */
	public boolean delete(String deltaObjectId, String measurementId, long timestamp, TSDataType type) {
		if (!checkRecord(deltaObjectId, measurementId, type))
			return false;
		overflowMap.get(deltaObjectId).get(measurementId).delete(timestamp);
		return true;
	}

	/**
	 * @param deltaObjectId
	 * @param measurementId
	 * @param timeFilter
	 * @param freqFilter
	 * @param valueFilter
	 * @return
	 */
	public List<Object> query(String deltaObjectId, String measurementId, SingleSeriesFilterExpression timeFilter,
			SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter) {
		List<Object> res;
		if (!checkRecordForQuery(deltaObjectId, measurementId)) {
			res = new ArrayList<>();
			res.add(null);
			res.add(null);
			res.add(null);
			res.add(timeFilter);
			return res;
		}

		res = overflowMap.get(deltaObjectId).get(measurementId).query(timeFilter, freqFilter, valueFilter);
		if (res == null) {
			res = new ArrayList<>();
			res.add(null);
			res.add(null);
			res.add(null);
			res.add(timeFilter);
		}
		return res;
	}

	/**
	 * Switch all the overFlowSeriesImpl from working to flushing
	 */
	public void switchWorkToFlush() {
		for (Map<String, OverflowSeriesImpl> overflowSeriesWriters : overflowMap.values()) {
			for (OverflowSeriesImpl overflowSeriesWriter : overflowSeriesWriters.values()) {
				overflowSeriesWriter.switchWorkingToFlushing();
			}
		}
	}

	/**
	 * Flush all the overflow series into OverflowFileIO
	 * 
	 * @throws IOException
	 */
	public void flushRowGroupToStore() throws IOException {
		// seek to tail of overflowFileIO
		fileWriter.toTail();
		// flush every overflowSeries to overflowFileIO
		for (Map<String, OverflowSeriesImpl> overflowSeriesWriters : overflowMap.values()) {
			for (OverflowSeriesImpl overflowSeriesWriter : overflowSeriesWriters.values()) {
				overflowSeriesWriter.flushToFileWriter(fileWriter);
			}
		}
	}

	/**
	 * While calling this method， the thread owns the write lock for the
	 * overflow processor
	 * 
	 * @throws IOException
	 */
	public void switchWorkToMerge() throws IOException {
		// should add return parameter to identity the .merge file
		// delete the overflow restore file
		if (!fileWriter.switchFileIOWorkingToMerge()) {
			lastFileOffset = 0;
			// should clear overflow map
			overflowMap.clear();
		}

		// get the the overflowMap from overflow.merge file
		OverflowFileIO mergeOverflowFileIO = fileWriter.getOverflowIOForMerge();
		OverflowSupport mergeOverflowSupport = new OverflowSupport(mergeOverflowFileIO);
		Map<String, Map<String, OverflowSeriesImpl>> mergeOverflowMap = mergeOverflowSupport.overflowMap;

		for (Entry<String, Map<String, OverflowSeriesImpl>> rowGroupWriterEntry : mergeOverflowMap.entrySet()) {
			String deltaObjectId = rowGroupWriterEntry.getKey();
			for (Entry<String, OverflowSeriesImpl> seriesWriterEntry : rowGroupWriterEntry.getValue().entrySet()) {
				String measurementId = seriesWriterEntry.getKey();
				if (overflowMap.containsKey(deltaObjectId)
						&& overflowMap.get(deltaObjectId).containsKey(measurementId)) {
					overflowMap.get(deltaObjectId).get(measurementId)
							.setMergingSeriesImpl(seriesWriterEntry.getValue());
				} else if (overflowMap.containsKey(deltaObjectId)) {
					OverflowSeriesImpl overflowSeriesImpl = new OverflowSeriesImpl(measurementId,
							seriesWriterEntry.getValue().getTSDataType(), fileWriter, compressor, null);
					overflowSeriesImpl.setMergingSeriesImpl(seriesWriterEntry.getValue());
					overflowMap.get(deltaObjectId).put(measurementId, overflowSeriesImpl);
				} else {
					OverflowSeriesImpl overflowSeriesImpl = new OverflowSeriesImpl(measurementId,
							seriesWriterEntry.getValue().getTSDataType(), fileWriter, compressor, null);
					overflowSeriesImpl.setMergingSeriesImpl(seriesWriterEntry.getValue());
					overflowMap.put(deltaObjectId, new HashMap<>());
					overflowMap.get(deltaObjectId).put(measurementId, overflowSeriesImpl);
				}
			}
		}
		
		for (Entry<String, Map<String, OverflowSeriesImpl>> rowGroupWriterEntry : overflowMap.entrySet()) {
			for (Entry<String, OverflowSeriesImpl> seriesWriterEntry : rowGroupWriterEntry.getValue().entrySet()) {
				//
				// check
				//
				if (seriesWriterEntry.getValue().hasMergingSeriesImpl()) {
					seriesWriterEntry.getValue().switchWorkingToMerging();
				}
			}
		}
	}

	// should modify by the overflowmergemap
	public void switchMergeToWork() throws IOException {
		// should switch all overflow Map?
		// why not only switch mergeoverflow map
		for (Entry<String, Map<String, OverflowSeriesImpl>> rowGroupWritersEntry : overflowMap.entrySet()) {
			for (Entry<String, OverflowSeriesImpl> seriesWritersEntry : rowGroupWritersEntry.getValue().entrySet()) {
				//
				// checke
				//
				if (seriesWritersEntry.getValue().hasMergingSeriesImpl()) {
					seriesWritersEntry.getValue().switchMergeToWorking();
				}
			}
		}
		// Close tempOverflowIOForMerge IO and delete overflow merge file
		// delete the overflowrestore file
		fileWriter.switchFileIOMergeToWorking();
	}

	/**
	 * Flush the new metadata to the tail of the overflow file. If no new
	 * metadata, it will not add the metadata to the tail of the overflow file.
	 * 
	 * @return -1-no new metadata, otherwise new metadata
	 * @throws IOException
	 */
	public long endFile() throws IOException {
		List<OFRowGroupListMetadata> ofRowGroupListMetadatas = new ArrayList<OFRowGroupListMetadata>();
		if (overflowMap != null) {
			for (Entry<String, Map<String, OverflowSeriesImpl>> rowGroupWriterEntry : overflowMap.entrySet()) {
				Map<String, OverflowSeriesImpl> seriesListWriterMap = rowGroupWriterEntry.getValue();
				OFRowGroupListMetadata ofRowGroupListMetadata = new OFRowGroupListMetadata(
						rowGroupWriterEntry.getKey());
				for (Entry<String, OverflowSeriesImpl> seriesImplEntry : seriesListWriterMap.entrySet()) {
					OverflowSeriesImpl seriesImpl = seriesImplEntry.getValue();
					if (!seriesImpl.isEmptyForWrite())
						ofRowGroupListMetadata.addSeriesListMetaData(seriesImpl.getOFSeriesListMetadata());
				}
				if (!ofRowGroupListMetadata.isEmptySeriesList()) {
					ofRowGroupListMetadatas.add(ofRowGroupListMetadata);
				}
			}
		}
		if (ofRowGroupListMetadatas.isEmpty()) {
			fileWriter.close();
			return -1;
		} else {
			return fileWriter.endOverflow(new OFFileMetadata(lastFileOffset, ofRowGroupListMetadatas));
		}
	}

	// should modify by overflowmergemap
	public OFFileMetadata getOFFileMetadata() throws IOException {
		List<OFRowGroupListMetadata> ofRowGroupListMetadatas = new ArrayList<OFRowGroupListMetadata>();
		if (overflowMap != null) {
			for (Entry<String, Map<String, OverflowSeriesImpl>> rowGroupWriterEntry : overflowMap.entrySet()) {
				Map<String, OverflowSeriesImpl> seriesListWriterMap = rowGroupWriterEntry.getValue();
				OFRowGroupListMetadata ofRowGroupListMetadata = new OFRowGroupListMetadata(
						rowGroupWriterEntry.getKey());
				for (Entry<String, OverflowSeriesImpl> seriesImplEntry : seriesListWriterMap.entrySet()) {
					OverflowSeriesImpl seriesImpl = seriesImplEntry.getValue();
					if (!seriesImpl.isEmptyForWrite())
						ofRowGroupListMetadata.addSeriesListMetaData(seriesImpl.getOFSeriesListMetadata());
				}
				if (!ofRowGroupListMetadata.isEmptySeriesList()) {
					ofRowGroupListMetadatas.add(ofRowGroupListMetadata);
				}
			}
		}
		if (ofRowGroupListMetadatas.isEmpty()) {
			throw new IOException("Get OFFileMetadata failed, the overflow ofRowGroupListMetadatas is empty");
		} else {
			return new OFFileMetadata(lastFileOffset, ofRowGroupListMetadatas);
		}
	}

	public long getPos() throws IOException {
		return fileWriter.getTail();
	}
}
