package cn.edu.tsinghua.tsfile.encoding.decoder;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.encoding.encoder.DoublePrecisionEncoder;
import cn.edu.tsinghua.tsfile.encoding.encoder.Encoder;
import cn.edu.tsinghua.tsfile.encoding.encoder.SinglePrecisionEncoder;

public class GorillaDecoderTest {
	private static final Logger LOGGER = LoggerFactory.getLogger(GorillaDecoderTest.class);
	private List<Float> floatList;
	private List<Double> doubleList;
	private final double delta = 0.0000001;
	private final int floatMaxPointValue = 10000;
	private final long doubleMaxPointValue = 1000000000000000L;

	@Before
	public void setUp() throws Exception {
		floatList = new ArrayList<Float>();	
		int hybridCount = 11;
		int hybridNum = 50;
		int hybridStart = 2000;
		for (int i = 0; i < hybridNum; i++) {
			for (int j = 0; j < hybridCount; j++) {
				floatList.add((float) hybridStart / floatMaxPointValue);
			}
			for (int j = 0; j < hybridCount; j++) {
				floatList.add((float) hybridStart / floatMaxPointValue);
				hybridStart += 3;
			}

			hybridCount += 2;
		}

		doubleList = new ArrayList<Double>();
		int hybridCountDouble = 11;
		int hybridNumDouble = 50;
		long hybridStartDouble = 2000;

		for (int i = 0; i < hybridNumDouble; i++) {
			for (int j = 0; j < hybridCountDouble; j++) {
				doubleList.add((double) hybridStartDouble / doubleMaxPointValue);
			}
			for (int j = 0; j < hybridCountDouble; j++) {
				doubleList.add((double) hybridStartDouble / doubleMaxPointValue);
				hybridStart += 3;
			}

			hybridCountDouble += 2;
		}
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testNegativeNumber() throws IOException {
		Encoder encoder = new SinglePrecisionEncoder();
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		float value = -7.101f;
		encoder.encode(value, baos);
		encoder.encode(value - 2, baos);
		encoder.encode(value - 4, baos);
		encoder.flush(baos);		
		encoder.encode(value, baos);
		encoder.encode(value - 2, baos);
		encoder.encode(value - 4, baos);
		encoder.flush(baos);
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		for(int i = 0; i < 2;i++){
			Decoder decoder = new SinglePrecisionDecoder();
			if(decoder.hasNext(bais)){
				assertEquals(value, decoder.readFloat(bais), delta);
			}
			if(decoder.hasNext(bais)){
				assertEquals(value-2, decoder.readFloat(bais), delta);
			}
			if(decoder.hasNext(bais)){
				assertEquals(value-4, decoder.readFloat(bais), delta);
			}
		}
	}
	
	@Test
	public void testZeroNumber() throws IOException{
		Encoder encoder = new DoublePrecisionEncoder();
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		double value = 0f;
		encoder.encode(value, baos);
		encoder.encode(value, baos);
		encoder.encode(value, baos);
		encoder.flush(baos);		
		encoder.encode(value, baos);
		encoder.encode(value, baos);
		encoder.encode(value, baos);
		encoder.flush(baos);
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		for(int i = 0; i < 2;i++){
			Decoder decoder = new DoublePrecisionDecoder();
			if(decoder.hasNext(bais)){
				assertEquals(value, decoder.readDouble(bais), delta);
			}
			if(decoder.hasNext(bais)){
				assertEquals(value, decoder.readDouble(bais), delta);
			}
			if(decoder.hasNext(bais)){
				assertEquals(value, decoder.readDouble(bais), delta);
			}
		}
	}
	
	@Test
	public void testFloatRepeat() throws Exception {
		for (int i = 1; i <= 10; i++) {
			testFloatLength(floatList, false, i);
		}
	}

	@Test
	public void testDoubleRepeat() throws Exception {
		for (int i = 1; i <= 10; i++) {
			testDoubleLength(doubleList, false, i);
		}
	}
	
	@Test
	public void testFloat() throws IOException {
		Encoder encoder = new SinglePrecisionEncoder();
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		float value = 7.101f;
		int num = 10000;
		for(int i = 0; i < num;i++){
			encoder.encode(value + 2 * i, baos);
		}
		encoder.flush(baos);
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		Decoder decoder = new SinglePrecisionDecoder();
		for(int i = 0; i < num;i++){
			if(decoder.hasNext(bais)){
				assertEquals(value + 2 * i, decoder.readFloat(bais), delta);
				continue;
			}
			fail();
		}
	}
	
	@Test
	public void testDouble() throws IOException {
		Encoder encoder = new DoublePrecisionEncoder();
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		double value = 7.101f;
		int num = 1000;
		for(int i = 0; i < num;i++){
			encoder.encode(value + 2 * i, baos);
		}
		encoder.flush(baos);
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		Decoder decoder = new DoublePrecisionDecoder();
		for(int i = 0; i < num;i++){
			if(decoder.hasNext(bais)){
//				System.out.println("turn "+i);
				assertEquals(value + 2 * i, decoder.readDouble(bais), delta);
				continue;
			}
			fail();
		}
	}

	private void testFloatLength(List<Float> valueList, boolean isDebug, int repeatCount) throws Exception {
		Encoder encoder = new SinglePrecisionEncoder();
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		for (int i = 0; i < repeatCount; i++) {
			for (float value : valueList) {
				encoder.encode(value, baos);
			}
			encoder.flush(baos);
		}
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		for (int i = 0; i < repeatCount; i++) {
			
			Decoder decoder = new SinglePrecisionDecoder();
			for (float value : valueList) {
//				System.out.println("Repeat: "+i+" value: "+value);
				if(decoder.hasNext(bais)){
					float value_ = decoder.readFloat(bais);
					if (isDebug) {
						LOGGER.debug("{} // {}", value_, value);
					}
					assertEquals(value, value_, delta);
					continue;
				}
				fail();
			}
		}
	}

	private void testDoubleLength(List<Double> valueList, boolean isDebug, int repeatCount) throws Exception {
		Encoder encoder = new DoublePrecisionEncoder();
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		for (int i = 0; i < repeatCount; i++) {
			for (double value : valueList) {
				encoder.encode(value, baos);
			}
			encoder.flush(baos);
		}

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());

		for (int i = 0; i < repeatCount; i++) {
			Decoder decoder = new DoublePrecisionDecoder();
			for (double value : valueList) {
				if(decoder.hasNext(bais)){
					double value_ = decoder.readDouble(bais);
					if (isDebug) {
						LOGGER.debug("{} // {}", value_, value);
					}
					assertEquals(value, value_, delta);
					continue;
				}
				fail();
			}
		}
	}
}
