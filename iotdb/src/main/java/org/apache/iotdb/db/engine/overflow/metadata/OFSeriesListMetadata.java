package org.apache.iotdb.db.engine.overflow.metadata;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;


/**
 * Metadata of overflow series list
 */
public class OFSeriesListMetadata{

	private String measurementId;
	private List<ChunkMetaData> timeSeriesList;

	private OFSeriesListMetadata() {
	}

	public OFSeriesListMetadata(String measurementId, List<ChunkMetaData> timeSeriesList) {
		this.measurementId = measurementId;
		this.timeSeriesList = timeSeriesList;
	}

	/**
	 * add TimeSeriesChunkMetaData to timeSeriesList
	 * 
	 * @param timeSeries
	 */
	public void addSeriesMetaData(ChunkMetaData timeSeries) {
		if (timeSeriesList == null) {
			timeSeriesList = new ArrayList<ChunkMetaData>();
		}
		timeSeriesList.add(timeSeries);
	}

	public List<ChunkMetaData> getMetaDatas() {
		return timeSeriesList == null ? null : Collections.unmodifiableList(timeSeriesList);
	}

	@Override
	public String toString() {
		return String.format("OFSeriesListMetadata{ measurementId id: %s, series: %s }", measurementId,
				timeSeriesList.toString());
	}

	public String getMeasurementId() {
		return measurementId;
	}

	public int serializeTo(OutputStream outputStream) throws IOException {
		int byteLen = 0;
		byteLen+= ReadWriteIOUtils.write(measurementId,outputStream);
		byteLen += ReadWriteIOUtils.write(timeSeriesList.size(), outputStream);
		for(ChunkMetaData chunkMetaData : timeSeriesList){
			byteLen+= chunkMetaData.serializeTo(outputStream);
		}
		return byteLen;
	}

	public int serializeTo(ByteBuffer buffer) throws IOException {
		throw new NotImplementedException();
	}

	public static OFSeriesListMetadata deserializeFrom(InputStream inputStream) throws IOException {
		OFSeriesListMetadata ofSeriesListMetadata = new OFSeriesListMetadata();
		ofSeriesListMetadata.measurementId = ReadWriteIOUtils.readString(inputStream);
		int size = ReadWriteIOUtils.readInt(inputStream);
		List<ChunkMetaData> list = new ArrayList<>();
		for(int i = 0;i<size;i++){
			ChunkMetaData chunkMetaData = ChunkMetaData.deserializeFrom(inputStream);
			list.add(chunkMetaData);
		}
		ofSeriesListMetadata.timeSeriesList = list;
		return ofSeriesListMetadata;
	}

	public static OFSeriesListMetadata deserializeFrom(ByteBuffer buffer) throws IOException {
		throw new NotImplementedException();
	}
}
