package cn.edu.tsinghua.iotdb.engine.overflow.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.edu.tsinghua.iotdb.engine.bufferwrite.FileNodeConstants;
import cn.edu.tsinghua.iotdb.engine.overflow.IIntervalTreeOperator;
import cn.edu.tsinghua.iotdb.engine.overflow.metadata.OFFileMetadata;
import cn.edu.tsinghua.iotdb.engine.overflow.metadata.OFRowGroupListMetadata;
import cn.edu.tsinghua.iotdb.engine.overflow.metadata.OFSeriesListMetadata;
import cn.edu.tsinghua.iotdb.query.aggregation.AggregationConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.engine.overflow.utils.OverflowReadWriteThriftFormatUtils;
import cn.edu.tsinghua.iotdb.engine.overflow.utils.TSFileMetaDataConverter;
import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TsDigest;
import cn.edu.tsinghua.tsfile.file.metadata.VInTimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSChunkType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.statistics.Statistics;

/**
 * OverflowFileIO support read and write.
 *
 * @author kangrong
 * @author liukun
 */
public class OverflowFileIO {
	private static final Logger LOGGER = LoggerFactory.getLogger(OverflowFileIO.class);
	public final TSFileMetaDataConverter metadataConverter = new TSFileMetaDataConverter();

	public static final int FOOTER_LENGTH = 4;
	public static final int OVERFLOW_VERSION = 0;
	private OverflowReadWriter raf;
	private final String normalFileNamePath;
	private TimeSeriesChunkMetaData currentSeries;

	// The parameter of normalFileNamePath is not useful
	public OverflowFileIO(OverflowReadWriter raf, String normalFileNamePath, long lastUpdateOffset) throws IOException {

		this.raf = raf;
		this.normalFileNamePath = normalFileNamePath;
		try {
			restoreBrokenFile(lastUpdateOffset);
		} catch (IOException e) {
			LOGGER.error("Restore broken file error, reason is {}.", e.getMessage());
			throw e;
		}
	}

	private void restoreBrokenFile(long lastUpdateOffset) throws IOException {

		long len = raf.length();
		if (lastUpdateOffset != len) {
			raf.cutOff(lastUpdateOffset);
		}
	}

	/**
	 * Get the overflow metadata which has been stored in the overflow file
	 * 
	 * @deprecated
	 * @return
	 * @throws IOException
	 */
	public Map<String, Map<String, List<TimeSeriesChunkMetaData>>> getSeriesListMap() throws IOException {
		Map<String, Map<String, List<TimeSeriesChunkMetaData>>> overflowMap = new HashMap<String, Map<String, List<TimeSeriesChunkMetaData>>>();

		long lastOffset = raf.length();
		int ofFileMetaDataLength;
		while (lastOffset != 0) {
			raf.seek(lastOffset - FOOTER_LENGTH);
			ofFileMetaDataLength = raf.readInt();

			LOGGER.debug("Read the overflow file, the last overflow block footer metadata size is :{}.",
					ofFileMetaDataLength);
			LOGGER.debug("The begin offset of the last overflow block footer metadata is :{}.",
					lastOffset - FOOTER_LENGTH - ofFileMetaDataLength);
			raf.seek(lastOffset - FOOTER_LENGTH - ofFileMetaDataLength);
			byte[] buf = new byte[ofFileMetaDataLength];
			raf.read(buf, 0, buf.length);

			ByteArrayInputStream bais = new ByteArrayInputStream(buf);
			OFFileMetadata ofFileMetadata = new TSFileMetaDataConverter()
					.toOFFileMetadata(OverflowReadWriteThriftFormatUtils.readOFFileMetaData(bais));
			lastOffset = ofFileMetadata.getLastFooterOffset();
			for (OFRowGroupListMetadata rowGroupListMeta : ofFileMetadata.getMetaDatas()) {
				String deltaObjectId = rowGroupListMeta.getDeltaObjectId();
				if (!overflowMap.containsKey(deltaObjectId))
					overflowMap.put(deltaObjectId, new HashMap<String, List<TimeSeriesChunkMetaData>>());
				Map<String, List<TimeSeriesChunkMetaData>> ofRowGroup = overflowMap.get(deltaObjectId);
				for (OFSeriesListMetadata seriesListMeta : rowGroupListMeta.getMetaDatas()) {
					String measurementId = seriesListMeta.getMeasurementId();
					if (!ofRowGroup.containsKey(measurementId))
						ofRowGroup.put(measurementId, new ArrayList<TimeSeriesChunkMetaData>());
					List<TimeSeriesChunkMetaData> seriesList = ofRowGroup.get(measurementId);
					List<TimeSeriesChunkMetaData> seriesListInFile = seriesListMeta.getMetaDatas();
					int index = seriesList.size();
					for (TimeSeriesChunkMetaData oneTimeSeriesChunkMetaData : seriesListInFile) {
						seriesList.add(index, oneTimeSeriesChunkMetaData);
					}
					// seriesList.addAll(seriesListInFile);
					LOGGER.debug("Init the old overflow deltaObjectId:{},measurementId:{},serieslist:{}.",
							deltaObjectId, measurementId, seriesListInFile);
				}
			}
		}

		return overflowMap;
	}

	public Map<String, Map<String, List<TimeSeriesChunkMetaData>>> getSeriesListMap(long lastFileOffset)
			throws IOException {

		Map<String, Map<String, List<TimeSeriesChunkMetaData>>> overflowMap = new HashMap<String, Map<String, List<TimeSeriesChunkMetaData>>>();
		// seek the offset
		// read data from overflow file
		long lastOffset = lastFileOffset;
		while (lastOffset != 0) {
			int ofFileMetaDataLength;
			raf.seek(lastOffset - FOOTER_LENGTH);
			ofFileMetaDataLength = raf.readInt();

			LOGGER.debug("Read the overflow file, the last overflow block footer metadata size is :{}.",
					ofFileMetaDataLength);
			LOGGER.debug("The begin offset of the last overflow block footer metadata is :{}.",
					lastOffset - FOOTER_LENGTH - ofFileMetaDataLength);
			byte[] buf = new byte[ofFileMetaDataLength];
			raf.seek(lastOffset - FOOTER_LENGTH - ofFileMetaDataLength);
			raf.read(buf, 0, buf.length);
			ByteArrayInputStream bais = new ByteArrayInputStream(buf);
			OFFileMetadata ofFileMetadata = new TSFileMetaDataConverter()
					.toOFFileMetadata(OverflowReadWriteThriftFormatUtils.readOFFileMetaData(bais));
			lastOffset = ofFileMetadata.getLastFooterOffset();
			for (OFRowGroupListMetadata rowGroupListMeta : ofFileMetadata.getMetaDatas()) {
				String deltaObjectId = rowGroupListMeta.getDeltaObjectId();
				if (!overflowMap.containsKey(deltaObjectId))
					overflowMap.put(deltaObjectId, new HashMap<String, List<TimeSeriesChunkMetaData>>());
				Map<String, List<TimeSeriesChunkMetaData>> ofRowGroup = overflowMap.get(deltaObjectId);
				for (OFSeriesListMetadata seriesListMeta : rowGroupListMeta.getMetaDatas()) {
					String measurementId = seriesListMeta.getMeasurementId();
					if (!ofRowGroup.containsKey(measurementId))
						ofRowGroup.put(measurementId, new ArrayList<TimeSeriesChunkMetaData>());
					List<TimeSeriesChunkMetaData> seriesList = ofRowGroup.get(measurementId);
					List<TimeSeriesChunkMetaData> seriesListInFile = seriesListMeta.getMetaDatas();
					int index = seriesList.size();
					for (TimeSeriesChunkMetaData oneTimeSeriesChunkMetaData : seriesListInFile) {
						seriesList.add(index, oneTimeSeriesChunkMetaData);
					}
					// seriesList.addAll(seriesListInFile);
					LOGGER.debug("Init the old overflow deltaObjectId:{},measurementId:{},serieslist:{}.",
							deltaObjectId, measurementId, seriesListInFile);
				}
			}
		}

		return overflowMap;
	}

	public InputStream getSeriesChunkBytes(int chunkSize, long offset) {
		try {
			raf.seek(offset);
			byte[] chunk = new byte[chunkSize];
			int off = 0;
			int len = chunkSize;
			while (len > 0) {
				int num = raf.read(chunk, off, len);
				off = off + num;
				len = len - num;
			}
			return new ByteArrayInputStream(chunk);
		} catch (IOException e) {
			LOGGER.error("Read series chunk failed, reason is {}", e.getMessage());
			return new ByteArrayInputStream(new byte[0]);
		}
	}

	/**
	 * This method is used to construct one overflow series metadata
	 * 
	 * @param valueCount
	 * @param measurementId
	 * @param compressionCodecName
	 * @param tsDataType
	 * @param statistics
	 * @throws IOException
	 */
	public void startSeries(int valueCount, String measurementId, CompressionTypeName compressionCodecName,
			TSDataType tsDataType, Statistics<?> statistics) throws IOException {
		LOGGER.debug(
				"Start overflow series chunk meatadata: measurementId: {}, valueCount: {}, compressionName: {}, TSdatatype: {}.",
				measurementId, valueCount, compressionCodecName, tsDataType);
		currentSeries = new TimeSeriesChunkMetaData(measurementId, TSChunkType.VALUE, raf.getPos(),
				compressionCodecName);
		currentSeries.setNumRows(valueCount);
		byte[] max = statistics.getMaxBytes();
		byte[] min = statistics.getMinBytes();
		VInTimeSeriesChunkMetaData vInTimeSeriesChunkMetaData = new VInTimeSeriesChunkMetaData(tsDataType);
		Map<String, ByteBuffer> minMaxMap = new HashMap<>();
		minMaxMap.put(AggregationConstant.MIN_VALUE, ByteBuffer.wrap(min));
		minMaxMap.put(AggregationConstant.MAX_VALUE, ByteBuffer.wrap(max));
		TsDigest tsDigest = new TsDigest(minMaxMap);
		vInTimeSeriesChunkMetaData.setDigest(tsDigest);
		currentSeries.setVInTimeSeriesChunkMetaData(vInTimeSeriesChunkMetaData);
	}

	public void writeOverflow(IIntervalTreeOperator overFlowIndex) throws IOException {
		ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
		overFlowIndex.toBytes(byteStream);
		OutputStream outputStream = raf.getOutputStream();
		byteStream.writeTo(outputStream);
		outputStream.flush();
	}

	public TimeSeriesChunkMetaData endSeries(long size) throws IOException {
		LOGGER.debug("End overflow series chunk meatadata: {}, size: {}.", currentSeries, size);
		currentSeries.setTotalByteSize(size);
		return currentSeries;
	}

	/**
	 * 
	 * @param footer
	 * @return
	 * @throws IOException
	 */
	public long endOverflow(OFFileMetadata footer) throws IOException {
		long footerBeginOffset = raf.toTail();
		OverflowReadWriteThriftFormatUtils.writeOFFileMetaData(
				metadataConverter.toThriftOFFileMetadata(OVERFLOW_VERSION, footer), raf.getOutputStream());
		LOGGER.debug("Serialize the overflow footer, footer length:{}, last overflow file offset: {}.",
				raf.getPos() - footerBeginOffset, footer.getLastFooterOffset());
		LOGGER.debug("Serialize the footer finished, file pos:{}.", raf.getPos());
		raf.write(BytesUtils.intToBytes((int) (raf.getPos() - footerBeginOffset)));
		long len = raf.getPos();
		LOGGER.debug(
				"Write overflow file block, last overflow file offset: {}, this overflow filemeta length: {},this end pos: {}.",
				footer.getLastFooterOffset(), len - footerBeginOffset - FOOTER_LENGTH, len);
		// close the stream
		close();
		return len;
	}

	public long getTail() throws IOException {
		return raf.length();
	}

	public void toTail() throws IOException {
		raf.toTail();
	}

	public void close() throws IOException {
		raf.close();
	}

	public long getPos() throws IOException {
		return raf.getPos();
	}

	private OverflowFileIO tempOverflowIOForMerge = null;

	public OverflowFileIO getOverflowIOForMerge() {
		return tempOverflowIOForMerge;
	}

	/**
	 * Switch this FileIO to ".merge" and create a new OverflowFileIO object
	 * pointing to merging normal file and merging error file.<br>
	 * The overflow processor will be close before call this method
	 * 
	 * @return
	 * @throws IOException
	 */
	public boolean switchFileIOWorkingToMerge() throws IOException {

		boolean hasMergeFile = true;
		String preFilePath = normalFileNamePath;
		String mergeFilePath = preFilePath + FileNodeConstants.PATH_SEPARATOR + FileNodeConstants.MREGE_EXTENSION;
		File preFile = new File(preFilePath);
		File mergeFile = new File(mergeFilePath);

		if (!mergeFile.exists()) {
			if (raf != null) {
				raf.close();
			}
			preFile.renameTo(mergeFile);
			// try to delete the overflow restore file
			String restoreFilePath = preFilePath + ".restore";
			File restoreFile = new File(restoreFilePath);
			restoreFile.delete();
			raf = new OverflowReadWriter(preFile);
			hasMergeFile = false;
		}

		tempOverflowIOForMerge = new OverflowFileIO(new OverflowReadWriter(mergeFile), mergeFilePath,
				mergeFile.length());

		return hasMergeFile;

	}

	/**
	 * Delete the temple overflow file
	 * 
	 * @throws IOException
	 */
	public void switchFileIOMergeToWorking() throws IOException {
		if (tempOverflowIOForMerge != null) {
			tempOverflowIOForMerge.close();
		}

		String mergeFilePath = normalFileNamePath + FileNodeConstants.PATH_SEPARATOR
				+ FileNodeConstants.MREGE_EXTENSION;
		File mergeFile = new File(mergeFilePath);
		if (mergeFile.exists())
			mergeFile.delete();
		tempOverflowIOForMerge = null;
	}
}
