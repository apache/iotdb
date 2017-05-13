package cn.edu.thu.tsfiledb.engine.filenode;

import java.util.List;

import cn.edu.thu.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;

/**
 * This is a structure for a query result.<br>
 * The result of query contains four parts.<br>
 * The first part is data in bufferwrite in index<br>
 * The second part is data in bufferwrite in rowgroups<br>
 * The third part is data in all file closed<br>
 * The fourth part is data in overflow<br>
 * 
 * @author liukun
 *
 */
public class QueryStructure {
	
	private final DynamicOneColumnData bufferwriteDataInMemory;

	private final List<RowGroupMetaData> bufferwriteDataInDisk;
	
	private final List<IntervalFileNode> bufferwriteDataInFiles;
	
	private final List<Object> allOverflowData;

	public QueryStructure(DynamicOneColumnData bufferwriteDataInMemory, List<RowGroupMetaData> bufferwriteDataInDisk,
			List<IntervalFileNode> bufferwriteDataInFiles, List<Object> allOverflowData) {
		this.bufferwriteDataInMemory = bufferwriteDataInMemory;
		this.bufferwriteDataInDisk = bufferwriteDataInDisk;
		this.bufferwriteDataInFiles = bufferwriteDataInFiles;
		this.allOverflowData = allOverflowData;
	}

	public DynamicOneColumnData getBufferwriteDataInMemory() {
		return bufferwriteDataInMemory;
	}

	public List<RowGroupMetaData> getBufferwriteDataInDisk() {
		return bufferwriteDataInDisk;
	}

	public List<IntervalFileNode> getBufferwriteDataInFiles() {
		return bufferwriteDataInFiles;
	}

	public List<Object> getAllOverflowData() {
		return allOverflowData;
	}
	
	public String toString(){
		return "FilesList: " + String.valueOf(bufferwriteDataInFiles) + "\n"
				+ "InsertData: " + (allOverflowData.get(0) != null ? ((DynamicOneColumnData)allOverflowData.get(0)).length : 0);
	}
	
	
}
