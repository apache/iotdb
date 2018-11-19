package cn.edu.tsinghua.iotdb.engine.bufferwrite;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;

public class BufferWriteIOTest {

	private BufferIO bufferWriteIO = null;
	private String filePath = "bufferwritepath";

	@Before
	public void setUp() throws Exception {
		bufferWriteIO = new BufferIO(new TsRandomAccessFileWriter(new File(filePath)), 0, new ArrayList<>());
	}

	@After
	public void tearDown() throws Exception {
		EnvironmentUtils.cleanDir(filePath);
	}

	@Test
	public void test() throws IOException {
		assertEquals(0, bufferWriteIO.getPos());
		assertEquals(0, bufferWriteIO.getAppendedRowGroupMetadata().size());

		// construct one rowgroup
		bufferWriteIO.startRowGroup("d1");
		bufferWriteIO.endRowGroup(1000, 1000);
		assertEquals(1, bufferWriteIO.getRowGroups().size());
		assertEquals(1, bufferWriteIO.getAppendedRowGroupMetadata().size());
		List<RowGroupMetaData> metadatas = bufferWriteIO.getAppendedRowGroupMetadata();
		RowGroupMetaData rowgroup = metadatas.get(0);
		assertEquals("d1", rowgroup.getDeltaObjectID());
		assertEquals(1000, rowgroup.getTotalByteSize());
		assertEquals(1000, rowgroup.getNumOfRows());
		// construct another two rowgroup
		bufferWriteIO.startRowGroup("d1");
		bufferWriteIO.endRowGroup(1000, 1000);

		bufferWriteIO.startRowGroup("d1");
		bufferWriteIO.endRowGroup(1000, 1000);

		bufferWriteIO.startRowGroup("d1");
		bufferWriteIO.endRowGroup(1000, 1000);
		metadatas = bufferWriteIO.getAppendedRowGroupMetadata();
		assertEquals(3, metadatas.size());
		FileSchema fileSchema = new FileSchema();
		bufferWriteIO.endFile(fileSchema);
	}

}
