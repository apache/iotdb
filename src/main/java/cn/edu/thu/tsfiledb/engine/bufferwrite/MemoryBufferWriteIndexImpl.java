package cn.edu.thu.tsfiledb.engine.bufferwrite;

import java.util.HashMap;
import java.util.Map;

import cn.edu.thu.tsfile.common.utils.Binary;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfile.timeseries.write.record.DataPoint;
import cn.edu.thu.tsfile.timeseries.write.record.TSRecord;

/**
 * Implement the {@code BufferWriteIndex}<br>
 * This class is used to store bufferwrite data in memory, also index data
 * easily<br>
 * 
 * @author kangrong
 * @author liukun
 *
 */
@Deprecated
public class MemoryBufferWriteIndexImpl implements BufferWriteIndex {
	private Map<String, DynamicOneColumnData> indexMap;

	public MemoryBufferWriteIndexImpl() {
		indexMap = new HashMap<String, DynamicOneColumnData>();
	}

	@Override
	public DynamicOneColumnData query(String deltaObjectId, String measurementId) {
		return indexMap.get(concatPath(deltaObjectId, measurementId));
	}

	@Override
	public void insert(TSRecord tsRecord) {
		String deltaObjectId = tsRecord.deltaObjectId;
		for (DataPoint dp : tsRecord.dataPointList) {
			String measurementId = dp.getMeasurementId();
			String path = concatPath(deltaObjectId, measurementId);
			if (!indexMap.containsKey(path))
				indexMap.put(path, new DynamicOneColumnData(dp.getType(), true));
			DynamicOneColumnData deltaMea = indexMap.get(path);
			Object value = dp.getValue();
			deltaMea.putTime(tsRecord.time);
			switch (dp.getType()) {
			case INT32:
				deltaMea.putInt((Integer) value);
				break;
			case INT64:
				deltaMea.putLong((Long) value);
				break;
			case FLOAT:
				deltaMea.putFloat((Float) value);
				break;
			case DOUBLE:
				deltaMea.putDouble((Double) value);
				break;
			case TEXT:
				deltaMea.putBinary((Binary) value);
				break;
			case BOOLEAN:
				deltaMea.putBoolean((boolean) value);
				break;
			default:
				throw new UnsupportedOperationException("no support type:" + dp.getType() + "record:" + tsRecord);
			}
		}
	}

	@Override
	public void clear() {
		indexMap.clear();
	}

	private String concatPath(String deltaObjectId, String measurementId) {
		return deltaObjectId + FileNodeConstants.PATH_SEPARATOR + measurementId;
	}

}
