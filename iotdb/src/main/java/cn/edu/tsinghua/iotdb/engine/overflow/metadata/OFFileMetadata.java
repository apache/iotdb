package cn.edu.tsinghua.iotdb.engine.overflow.metadata;


import cn.edu.tsinghua.tsfile.utils.ReadWriteIOUtils;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class OFFileMetadata {

	private long lastFooterOffset;
	private List<OFRowGroupListMetadata> rowGroupLists;

	public OFFileMetadata() {
	}

	public OFFileMetadata(long lastFooterOffset, List<OFRowGroupListMetadata> rowGroupLists) {
		this.lastFooterOffset = lastFooterOffset;
		this.rowGroupLists = rowGroupLists;
	}

	/**
	 * add OFRowGroupListMetadata to list
	 * 
	 * @param rowGroupListMetadata
	 * @return void
	 */
	public void addRowGroupListMetaData(OFRowGroupListMetadata rowGroupListMetadata) {
		if (rowGroupLists == null) {
			rowGroupLists = new ArrayList<OFRowGroupListMetadata>();
		}
		rowGroupLists.add(rowGroupListMetadata);
	}

	public List<OFRowGroupListMetadata> getRowGroupLists() {
		return rowGroupLists == null ? null : Collections.unmodifiableList(rowGroupLists);
	}

	public long getLastFooterOffset() {
		return lastFooterOffset;
	}

	public void setLastFooterOffset(long lastFooterOffset) {
		this.lastFooterOffset = lastFooterOffset;
	}

	@Override
	public String toString() {
		return String.format("OFFileMetadata{ last offset: %d, RowGroupLists: %s }", lastFooterOffset,
				rowGroupLists.toString());
	}

	public int serializeTo(OutputStream outputStream) throws IOException {
		int byteLen = 0;
		byteLen += ReadWriteIOUtils.write(lastFooterOffset,outputStream);
		int size = rowGroupLists.size();
		byteLen += ReadWriteIOUtils.write(size,outputStream);
		for(OFRowGroupListMetadata ofRowGroupListMetadata : rowGroupLists){
			byteLen += ofRowGroupListMetadata.serializeTo(outputStream);
		}
		return byteLen;
	}

	public int serializeTo(ByteBuffer buffer) throws IOException {
		throw new NotImplementedException();
	}

	public static OFFileMetadata deserializeFrom(InputStream inputStream) throws IOException {
		long lastFooterOffset = ReadWriteIOUtils.readLong(inputStream);
		int size = ReadWriteIOUtils.readInt(inputStream);
		List<OFRowGroupListMetadata> list = new ArrayList<>();
		for(int i = 0;i<size;i++){
			list.add(OFRowGroupListMetadata.deserializeFrom(inputStream));
		}
		return new OFFileMetadata(lastFooterOffset,list);
	}

	public static OFFileMetadata deserializeFrom(ByteBuffer buffer) throws IOException {
		throw new NotImplementedException();
	}

}
