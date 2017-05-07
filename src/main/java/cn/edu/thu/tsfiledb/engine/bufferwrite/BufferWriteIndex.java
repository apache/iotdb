package cn.edu.thu.tsfiledb.engine.bufferwrite;

import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfile.timeseries.write.record.TSRecord;

/**
 * The function of this interface is to store and index TSRecord in memory
 * temporarily
 * 
 * @author kangrong
 *
 */
public interface BufferWriteIndex {
	/**
	 * insert a tsRecord
	 * 
	 * @param tsRecord
	 */
	void insert(TSRecord tsRecord);

	/**
	 * Get the DynamicOneColumnData from the buffer index
	 * 
	 * @param deltaObjectId
	 * @param measurementId
	 * @return
	 */
	public DynamicOneColumnData query(String deltaObjectId, String measurementId);

	/**
	 * clear all data written in the bufferindex structure which will be used
	 * for next stage
	 */
	void clear();
}
