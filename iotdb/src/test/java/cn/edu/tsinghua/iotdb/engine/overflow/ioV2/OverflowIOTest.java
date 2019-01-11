package cn.edu.tsinghua.iotdb.engine.overflow.ioV2;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import cn.edu.tsinghua.tsfile.read.reader.TsFileInput;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

public class OverflowIOTest {

	private String overflowFilePath = "overflowfile";
	private OverflowIO io = null;
	private TsFileInput reader = null;

	@Before
	public void setUp() throws Exception {
		io = new OverflowIO(new OverflowIO.OverflowReadWriter(overflowFilePath));
		reader = new OverflowIO.OverflowReadWriter(overflowFilePath);
	}

	@After
	public void tearDown() throws Exception {
		io.close();
		reader.close();
		File file = new File(overflowFilePath);
		file.delete();
	}

	@Test
	public void testFileCutoff() throws IOException {
		File file = new File("testoverflowfile");
		FileOutputStream fileOutputStream = new FileOutputStream(file);
		byte[] bytes = new byte[20];
		fileOutputStream.write(bytes);
		fileOutputStream.close();
		assertEquals(20, file.length());
		OverflowIO overflowIO = new OverflowIO(new OverflowIO.OverflowReadWriter(file.getPath()));
		assertEquals(20, file.length());
		overflowIO.close();
		file.delete();
	}

}
