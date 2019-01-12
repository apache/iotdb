package org.apache.iotdb.db.metadata;

import java.util.List;
import java.util.Map;

import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.PathErrorException;

/**
 * This class stores all the metadata info for every deviceId and every
 * timeseries
 */
public class Metadata {
	private Map<String, List<ColumnSchema>> seriesMap;
	private Map<String, List<String>> deviceIdMap;

	public Metadata(Map<String, List<ColumnSchema>> seriesMap, Map<String, List<String>> deviceIdMap) {
		this.seriesMap = seriesMap;
		this.deviceIdMap = deviceIdMap;
	}

	public List<ColumnSchema> getSeriesForOneType(String type) throws PathErrorException {
		if (this.seriesMap.containsKey(type)) {
			return seriesMap.get(type);
		} else {
			throw new PathErrorException("Input deviceIdType is not exist. " + type);
		}
	}

	public List<String> getDevicesForOneType(String type) throws PathErrorException {
		if (this.seriesMap.containsKey(type)) {
			return deviceIdMap.get(type);
		} else {
			throw new PathErrorException("Input deviceIdType is not exist. " + type);
		}
	}

	public Map<String, List<ColumnSchema>> getSeriesMap() {
		return seriesMap;
	}

	public Map<String, List<String>> getDeviceMap() {
		return deviceIdMap;
	}

	@Override
	public String toString() {
		return seriesMap.toString() + "\n" + deviceIdMap.toString();
	}

}
