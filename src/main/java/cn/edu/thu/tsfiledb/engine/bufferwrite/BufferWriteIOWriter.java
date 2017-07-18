package cn.edu.thu.tsfiledb.engine.bufferwrite;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileWriter;
import cn.edu.thu.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.thu.tsfile.timeseries.write.io.TSFileIOWriter;
import cn.edu.thu.tsfile.timeseries.write.schema.FileSchema;

/**
 * @author kangrong
 *
 */
public class BufferWriteIOWriter extends TSFileIOWriter {

	/*
	 * The backup list is used to store the rowgroup's metadata whose data has
	 * been flushed into file.
	 */
	private final List<RowGroupMetaData> backUpList = new ArrayList<RowGroupMetaData>();
	private int lastRowGroupIndex = 0;

	public BufferWriteIOWriter(FileSchema schema, TSRandomAccessFileWriter output) throws IOException {
		super(schema, output);
	}
	
	/**
	 * This is just used to restore a tsfile from the middle of the file
	 * @param schema
	 * @param output
	 * @param rowGroups
	 * @throws IOException
	 */
	public BufferWriteIOWriter(FileSchema schema,TSRandomAccessFileWriter output, long offset,List<RowGroupMetaData> rowGroups) throws IOException{
		super(schema, output,offset, rowGroups);
		addrowGroupsTobackupList(rowGroups);
		
	}
	
	private void addrowGroupsTobackupList(List<RowGroupMetaData> rowGroups){
		for(RowGroupMetaData rowGroupMetaData:rowGroups){
			backUpList.add(rowGroupMetaData);
		}
		lastRowGroupIndex = rowGroups.size();
	}

	/**
	 * <b>Note that</b>,the method is not thread safe.
	 */
	public void addNewRowGroupMetaDataToBackUp() {
		for(int i = lastRowGroupIndex;i<rowGroups.size();i++){
			backUpList.add(rowGroups.get(i));
		}
		lastRowGroupIndex = rowGroups.size();
	}

	/**
	 * <b>Note that</b>, the method is not thread safe. You mustn't do any
	 * change on the return.<br>
	 *
	 * @return
	 */
	public List<RowGroupMetaData> getCurrentRowGroupMetaList() {
		List<RowGroupMetaData> ret = new ArrayList<>();
		backUpList.forEach(ret::add);
		return ret;
	}
}
