package cn.edu.thu.tsfiledb.query.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileReader;
import cn.edu.thu.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.thu.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.read.ValueReader;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;

public class RowGroupReader{

	protected static final Logger logger = LoggerFactory.getLogger(RowGroupReader.class);
	public HashMap<String, TSDataType> seriesTypeMap;
	protected HashMap<String, OverflowValueReader> valueReaders = new HashMap<>();
	protected String deltaObjectUID;

	int lastRetIndex = -1;
	protected ArrayList<String> sids;
	protected String deltaObjectType;
	protected long totalByteSize;

	protected TSRandomAccessFileReader raf;
	
	public RowGroupReader(RowGroupMetaData rowGroupMetaData, TSRandomAccessFileReader raf) {
		logger.debug("init a new RowGroupReader..");
		seriesTypeMap = new HashMap<>();
		deltaObjectUID = rowGroupMetaData.getDeltaObjectUID();
		sids = new ArrayList<String>();
		deltaObjectType = rowGroupMetaData.getDeltaObjectType();
		this.totalByteSize = rowGroupMetaData.getTotalByteSize();
		this.raf = raf;
		
		for (TimeSeriesChunkMetaData tscMetaData : rowGroupMetaData.getTimeSeriesChunkMetaDataList()) {
			if (tscMetaData.getVInTimeSeriesChunkMetaData() != null) {
				sids.add(tscMetaData.getProperties().getMeasurementUID());
				seriesTypeMap.put(tscMetaData.getProperties().getMeasurementUID(),
						tscMetaData.getVInTimeSeriesChunkMetaData().getDataType());
				
				OverflowValueReader si = new OverflowValueReader(tscMetaData.getProperties().getFileOffset(),
                        tscMetaData.getTotalByteSize(),
                        tscMetaData.getVInTimeSeriesChunkMetaData().getDataType(),
                        tscMetaData.getVInTimeSeriesChunkMetaData().getDigest(), this.raf,
                        tscMetaData.getVInTimeSeriesChunkMetaData().getEnumValues(),
                        tscMetaData.getProperties().getCompression(), tscMetaData.getNumRows());
				valueReaders.put(tscMetaData.getProperties().getMeasurementUID(), si);
			}
		}
	}

	public List<Object> getTimeByRet(List<Object> timeRet, HashMap<Integer, Object> retMap) {
		List<Object> timeRes = new ArrayList<Object>();
		for (Integer i : retMap.keySet()) {
			timeRes.add(timeRet.get(i));
		}
		return timeRes;
	}

	public String getDeltaObjectType() {
		return this.deltaObjectType;
	}

	public TSDataType getDataTypeBySeriesName(String name) {
		return this.seriesTypeMap.get(name);
	}

	public String getDeltaObjectUID() {
		return this.deltaObjectUID;
	}
	
	/**
	 * Read time-value pairs whose time is be included in timeRet. WARNING: this
	 * function is only for "time" Series
	 * 
	 * @param sid measurement's id
	 * @param timeRet Array of the time.
	 * @throws IOException
	 */
    public DynamicOneColumnData readValueUseTimeValue(String measurementId, long[] timeRet) throws IOException{
    	DynamicOneColumnData v = valueReaders.get(measurementId).getValuesForGivenValues(timeRet);
    	return v;
    }

    public DynamicOneColumnData readOneColumnUseFilter(String sid, DynamicOneColumnData res, int fetchSize 
    		, SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter) throws IOException {
        ValueReader valueReader = valueReaders.get(sid);
        return valueReader.readOneColumnUseFilter(res, fetchSize, timeFilter,freqFilter, valueFilter);
    }
    
    public DynamicOneColumnData readOneColumn(String sid, DynamicOneColumnData res, int fetchSize) throws IOException {
        ValueReader valueReader = valueReaders.get(sid);
        return valueReader.readOneColumn(res, fetchSize);
    }
    
    public ValueReader getValueReaderForSpecificMeasurement(String sid) {
        return getValueReaders().get(sid);
    }

	public long getTotalByteSize() {
		return totalByteSize;
	}

	public void setTotalByteSize(long totalByteSize) {
		this.totalByteSize = totalByteSize;
	}

	public HashMap<String, OverflowValueReader> getValueReaders() {
		return valueReaders;
	}

	public void setValueReaders(HashMap<String, OverflowValueReader> valueReaders) {
		this.valueReaders = valueReaders;
	}

	public TSRandomAccessFileReader getRaf() {
		return raf;
	}

	public void setRaf(TSRandomAccessFileReader raf) {
		this.raf = raf;
	}
	
}
