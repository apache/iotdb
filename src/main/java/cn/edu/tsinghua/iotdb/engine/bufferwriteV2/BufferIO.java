package cn.edu.tsinghua.iotdb.engine.bufferwriteV2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;

public class BufferIO extends TsFileIOWriter {

	private int lastRowGroupIndex = 0;
	private List<RowGroupMetaData> append;

	public BufferIO(ITsRandomAccessFileWriter output, long offset, List<RowGroupMetaData> rowGroups)
			throws IOException {
		super(output, offset, rowGroups);
		lastRowGroupIndex = rowGroups.size();
		append = new ArrayList<>();
	}

	public List<RowGroupMetaData> getAppendedRowGroupMetadata() {
		if (lastRowGroupIndex < getRowGroups().size()) {
			append.clear();
			List<RowGroupMetaData> all = getRowGroups();
			for (int i = lastRowGroupIndex; i < all.size(); i++) {
				append.add(all.get(i));
			}
			lastRowGroupIndex = all.size();
		}
		return append;
	}

	public long getPos() throws IOException {
		return super.getPos();
	}
}
