package cn.edu.tsinghua.iotdb.engine.overflow.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.converter.IConverter;

/**
 * This is the metadata for overflow file. More information see
 * {@code com.corp.delta.tsfile.format.OFSeriesListMetadata}
 * 
 * @author liukun
 *
 */
public class OFSeriesListMetadata implements IConverter<cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFSeriesListMetadata> {
	private static final Logger LOGGER = LoggerFactory.getLogger(OFSeriesListMetadata.class);

	private String measurementId;
	private List<TimeSeriesChunkMetaData> timeSeriesList;

	public OFSeriesListMetadata() {
	}

	public OFSeriesListMetadata(String measurementId, List<TimeSeriesChunkMetaData> timeSeriesList) {
		this.measurementId = measurementId;
		this.timeSeriesList = timeSeriesList;
	}

	/**
	 * add TimeSeriesChunkMetaData to timeSeriesList
	 * 
	 * @param timeSeries
	 */
	public void addSeriesMetaData(TimeSeriesChunkMetaData timeSeries) {
		if (timeSeriesList == null) {
			timeSeriesList = new ArrayList<TimeSeriesChunkMetaData>();
		}
		timeSeriesList.add(timeSeries);
	}

	public List<TimeSeriesChunkMetaData> getMetaDatas() {
		return timeSeriesList == null ? null : Collections.unmodifiableList(timeSeriesList);
	}

	@Override
	public String toString() {
		return String.format("OFSeriesListMetadata{ measurementId id: %s, series: %s }", measurementId,
				timeSeriesList.toString());
	}

	public void setMeasurementId(String measurementId) {
		this.measurementId = measurementId;
	}

	public String getMeasurementId() {
		return measurementId;
	}

	@Override
	public cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFSeriesListMetadata convertToThrift() {
		try {
			List<cn.edu.tsinghua.tsfile.format.TimeSeriesChunkMetaData> tsc_metadata = null;
			if (timeSeriesList != null) {
				tsc_metadata = new ArrayList<cn.edu.tsinghua.tsfile.format.TimeSeriesChunkMetaData>();
				for (TimeSeriesChunkMetaData timeSeriesMetaData : timeSeriesList) {
					tsc_metadata.add(timeSeriesMetaData.convertToThrift());
				}
			}
			cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFSeriesListMetadata metaData = new cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFSeriesListMetadata(
					tsc_metadata, measurementId);
			return metaData;
		} catch (Exception e) {
			LOGGER.error("failed to convert OFSensorList metadata from TSF to thrift, series list metadata:{}",
					toString(), e);
			throw e;
		}
	}

	@Override
	public void convertToTSF(cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFSeriesListMetadata metadata) {
		try {
			measurementId = metadata.getMeasurement_id();
			List<cn.edu.tsinghua.tsfile.format.TimeSeriesChunkMetaData> timeSeriesChunkMetaDatalist = metadata
					.getTsc_metadata();
			if (timeSeriesChunkMetaDatalist == null) {
				timeSeriesList = null;
			} else {
				if (timeSeriesList == null) {
					timeSeriesList = new ArrayList<TimeSeriesChunkMetaData>();
				}
				timeSeriesList.clear();
				for (cn.edu.tsinghua.tsfile.format.TimeSeriesChunkMetaData formatSeries : timeSeriesChunkMetaDatalist) {
					TimeSeriesChunkMetaData seriesMetaData = new TimeSeriesChunkMetaData();
					seriesMetaData.convertToTSF(formatSeries);
					timeSeriesList.add(seriesMetaData);
				}
			}
		} catch (Exception e) {
			LOGGER.error("failed to convert OFSensorList metadata from thrift to TSF, series list metadata:{}",
					metadata.toString(), e);
			throw e;
		}
	}
}
