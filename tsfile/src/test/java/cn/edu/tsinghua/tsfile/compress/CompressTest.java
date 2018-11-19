package cn.edu.tsinghua.tsfile.compress;

import cn.edu.tsinghua.tsfile.common.utils.ListByteArrayOutputStream;
import cn.edu.tsinghua.tsfile.common.utils.PublicBAOS;
import cn.edu.tsinghua.tsfile.compress.UnCompressor.NoUnCompressor;
import cn.edu.tsinghua.tsfile.compress.UnCompressor.SnappyUnCompressor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xerial.snappy.Snappy;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * 
 * @author kangrong
 *
 */
public class CompressTest {
	private final String inputString = "Hello snappy-java! Snappy-java is a JNI-based wrapper of "
			+ "Snappy, a fast compressor/decompressor.";

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void noCompressorTest() throws IOException {
		PublicBAOS out = new PublicBAOS();
		out.write(inputString.getBytes("UTF-8"));
		Compressor.NoCompressor compressor = new Compressor.NoCompressor();
		NoUnCompressor unCompressor = new NoUnCompressor();
		ListByteArrayOutputStream compressed = compressor.compress(ListByteArrayOutputStream.from(out));
		byte[] uncompressed = unCompressor.uncompress(compressed.toByteArray());
		String result = new String(uncompressed, "UTF-8");
		assertEquals(inputString, result);
	}

	@Test
	public void snappyCompressorTest() throws IOException {
		PublicBAOS out = new PublicBAOS();
		out.write(inputString.getBytes("UTF-8"));
		Compressor.SnappyCompressor compressor = new Compressor.SnappyCompressor();
		SnappyUnCompressor unCompressor = new SnappyUnCompressor();
		ListByteArrayOutputStream compressed = compressor.compress(ListByteArrayOutputStream.from(out));
		byte[] uncompressed = unCompressor.uncompress(compressed.toByteArray());
		String result = new String(uncompressed, "UTF-8");
		assertEquals(inputString, result);
	}

	@Test
	public void snappyTest() throws IOException {
		byte[] compressed = Snappy.compress(inputString.getBytes("UTF-8"));
		byte[] uncompressed = Snappy.uncompress(compressed);

		String result = new String(uncompressed, "UTF-8");
		assertEquals(inputString, result);
	}

}
