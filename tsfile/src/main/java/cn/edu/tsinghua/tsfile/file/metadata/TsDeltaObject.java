package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.metadata.converter.IConverter;
import cn.edu.tsinghua.tsfile.format.DeltaObject;

public class TsDeltaObject implements IConverter<DeltaObject>{
	/** start position of RowGroupMetadataBlock in file **/
	public long offset;

	/** size of RowGroupMetadataBlock in byte **/
	public int metadataBlockSize;

	/** start time for a delta object **/
	public long startTime;

	/** end time for a delta object **/
	public long endTime;
	
	public TsDeltaObject(long offset, int metadataBlockSize, long startTime, long endTime){
		this.offset = offset;
		this.metadataBlockSize = metadataBlockSize;
		this.startTime = startTime;
		this.endTime = endTime;
	}

	@Override
	public DeltaObject convertToThrift() {
		return new DeltaObject(offset, metadataBlockSize, startTime, endTime);
	}

	@Override
	public void convertToTSF(DeltaObject metadata) {
		this.offset = metadata.getOffset();
		this.metadataBlockSize = metadata.getMetadata_block_size();
		this.startTime = metadata.getStart_time();
		this.endTime = metadata.getEnd_time();
	}
}
