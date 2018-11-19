package cn.edu.tsinghua.iotdb.engine.overflow.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.file.metadata.converter.IConverter;

/**
 * This is metadata for overflow file. More information see
 * {@code com.corp.delta.tsfile.format.OFRowGroupListMetadata}
 * 
 * @author liukun
 *
 */
public class OFRowGroupListMetadata implements IConverter<cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFRowGroupListMetadata> {
	private static final Logger LOGGER = LoggerFactory.getLogger(OFRowGroupListMetadata.class);

	private String deltaObjectId;
	private List<OFSeriesListMetadata> seriesLists;

	public OFRowGroupListMetadata() {
	}

	public OFRowGroupListMetadata(String deltaObjectId) {
		this.deltaObjectId = deltaObjectId;
	}

	
	/**
	 *  add OFSeriesListMetadata metadata to list
	 * 
	 * @param timeSeries
	 */
	public void addSeriesListMetaData(OFSeriesListMetadata timeSeries) {
		if (seriesLists == null) {
			seriesLists = new ArrayList<OFSeriesListMetadata>();
		}
		seriesLists.add(timeSeries);
	}

	public List<OFSeriesListMetadata> getMetaDatas() {
		return seriesLists == null ? null : Collections.unmodifiableList(seriesLists);
	}

	@Override
	public String toString() {
		return String.format("OFRowGroupListMetadata{ deltaObject id: %s, series Lists: %s }", deltaObjectId,
				seriesLists.toString());
	}

	public void setDeltaObjectId(String deltaObjectId) {
		this.deltaObjectId = deltaObjectId;
	}

	public String getDeltaObjectId() {
		return deltaObjectId;
	}

	public boolean isEmptySeriesList() {
		return (seriesLists == null || seriesLists.isEmpty());
	}

	@Override
	public cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFRowGroupListMetadata convertToThrift() {
		try {
			List<cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFSeriesListMetadata> fmSeriesList = null;
			if (seriesLists != null) {
				fmSeriesList = new ArrayList<cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFSeriesListMetadata>();
				for (OFSeriesListMetadata sensorListMetaData : seriesLists) {
					fmSeriesList.add(sensorListMetaData.convertToThrift());
				}
			}
			cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFRowGroupListMetadata metaData = new cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFRowGroupListMetadata(
					fmSeriesList, deltaObjectId);
			return metaData;
		} catch (Exception e) {
			LOGGER.error("failed to convert OFRowGroupList metadata from TSF to thrift, series lists metadata:{}",
					toString(), e);
			throw e;
		}
	}

	@Override
	public void convertToTSF(cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFRowGroupListMetadata metadata) {
		try {
			deltaObjectId = metadata.getDeltaObject_id();
			List<cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFSeriesListMetadata> list = metadata.getMeasurement_metadata();
			if (list == null) {
				seriesLists = null;
			} else {
				if (seriesLists == null) {
					seriesLists = new ArrayList<OFSeriesListMetadata>();
				}
				seriesLists.clear();
				for (cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFSeriesListMetadata formatSeries : list) {
					OFSeriesListMetadata seriesListMetaData = new OFSeriesListMetadata();
					seriesListMetaData.convertToTSF(formatSeries);
					seriesLists.add(seriesListMetaData);
				}
			}
		} catch (Exception e) {
			LOGGER.error("failed to convert OFRowGroupList from thrift to TSF, series lists metadata:{}",
					metadata.toString(), e);
			throw e;
		}
	}

	public List<OFSeriesListMetadata> getSeriesLists() {
		return this.seriesLists;
	}
}
