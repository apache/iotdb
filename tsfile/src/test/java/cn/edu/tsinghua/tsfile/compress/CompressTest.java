package cn.edu.tsinghua.tsfile.compress;

import cn.edu.tsinghua.tsfile.utils.PublicBAOS;
import cn.edu.tsinghua.tsfile.compress.UnCompressor.SnappyUnCompressor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.util.Arrays;

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
	public void setUp() {
	}

	@After
	public void tearDown() {
	}


	@Test
	public void snappyCompressorTest1() throws IOException {
		PublicBAOS out = new PublicBAOS();
		out.write(inputString.getBytes("UTF-8"));
		Compressor.SnappyCompressor compressor = new Compressor.SnappyCompressor();
		SnappyUnCompressor unCompressor = new SnappyUnCompressor();
		byte[] compressed = compressor.compress(out.getBuf());
		byte[] uncompressed = unCompressor.uncompress(compressed);
		String result = new String(uncompressed, "UTF-8");
		assertEquals(inputString, result);
	}
	@Test
	public void snappyCompressorTest2() throws IOException {
		PublicBAOS out = new PublicBAOS();
		out.write(inputString.getBytes("UTF-8"));
		Compressor.SnappyCompressor compressor = new Compressor.SnappyCompressor();
		SnappyUnCompressor unCompressor = new SnappyUnCompressor();
		byte[] compressed = new byte[compressor.getMaxBytesForCompression(out.size())];
		int size = compressor.compress(out.getBuf(),0, out.size(), compressed);
		byte[] bytes = Arrays.copyOfRange(compressed, 0, size);
		byte[] uncompressed = unCompressor.uncompress(bytes);
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
