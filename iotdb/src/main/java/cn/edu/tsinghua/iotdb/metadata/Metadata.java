package cn.edu.tsinghua.iotdb.metadata;

import java.util.List;
import java.util.Map;

import cn.edu.tsinghua.iotdb.exception.PathErrorException;

/**
 * This class stores all the metadata info for every deltaObject and every
 * timeseries
 * 
 * @author Jinrui Zhang
 *
 */
public class Metadata {
	private Map<String, List<ColumnSchema>> seriesMap;
	private Map<String, List<String>> deltaObjectMap;

	public Metadata(Map<String, List<ColumnSchema>> seriesMap, Map<String, List<String>> deltaObjectMap) {
		this.seriesMap = seriesMap;
		this.deltaObjectMap = deltaObjectMap;
	}

	public List<ColumnSchema> getSeriesForOneType(String type) throws PathErrorException {
		if (this.seriesMap.containsKey(type)) {
			return seriesMap.get(type);
		} else {
			throw new PathErrorException("Input DeltaObjectType is not exist. " + type);
		}
	}

	public List<String> getDeltaObjectsForOneType(String type) throws PathErrorException {
		if (this.seriesMap.containsKey(type)) {
			return deltaObjectMap.get(type);
		} else {
			throw new PathErrorException("Input DeltaObjectType is not exist. " + type);
		}
	}

	public Map<String, List<ColumnSchema>> getSeriesMap() {
		return seriesMap;
	}

	public Map<String, List<String>> getDeltaObjectMap() {
		return deltaObjectMap;
	}

	@Override
	public String toString() {
		return seriesMap.toString() + "\n" + deltaObjectMap.toString();
	}

}
