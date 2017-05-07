package cn.edu.thu.tsfiledb.engine.overflow.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertArrayEquals;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.thu.tsfiledb.engine.overflow.io.OverflowReadWriter;

public class OverflowReadWriterTest {
	
	private String filePath  = "overflowreadwritertest";
	private String backupFilePath = filePath+".backup";
	private OverflowReadWriter ofIO = null;
	
	

	@Before
	public void setUp() throws Exception {
		EngineTestHelper.delete(filePath);
		EngineTestHelper.delete(backupFilePath);
	}

	@After
	public void tearDown() throws Exception {
		EngineTestHelper.delete(filePath);
		EngineTestHelper.delete(backupFilePath);
	}

	@Test
	public void testCutoff() {
		
		try {
			ofIO = new OverflowReadWriter(filePath);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Construct the instance of overflowReadWriter failed");
		}
		assertEquals(filePath, ofIO.getFilePath());
		checkFile(filePath, 0);
		byte[] writebytes = writeBytes(ofIO, 10);
		checkFile(filePath, 10);
		try {
			assertEquals(10, ofIO.length());
			assertEquals(10, ofIO.getPos());
		} catch (IOException e2) {
			e2.printStackTrace();
			fail("Get tail or get pos failed");
		}
		try {
			ofIO.seek(0);
		} catch (IOException e2) {
			e2.printStackTrace();
			fail("The reason is "+e2.getMessage());
		}
		
		byte[] readbytes = readBytes(ofIO,10);
		assertArrayEquals(readbytes, writebytes);
		
		try {
			assertEquals(10, ofIO.toTail());
		} catch (IOException e1) {
			e1.printStackTrace();
			fail("To tail error");
		}
		try {
			ofIO.cutOff(5);
		} catch (IOException e) {

			e.printStackTrace();
			fail("cut off file failed, the reason is "+e.getMessage());
		}
		checkFile(filePath, 5);
	}
	
	
	@Test
	public void testReadAnaWrite(){
		try {
			ofIO = new OverflowReadWriter(filePath);
			ofIO.write(10);
			ofIO.close();
			ofIO = new OverflowReadWriter(filePath);
			assertEquals(10, ofIO.read());
			ofIO.close();
		} catch (IOException e) {
			e.printStackTrace();
			fail("Reason is "+e.getMessage());
		}
		
	}
	
	private void checkFile(String filePath,int length){
		File file = new File(filePath);
		assertEquals(true, file.exists());
		assertEquals(length, file.length());
	}
	
	private byte[] readBytes(OverflowReadWriter ofIO, int num){
		byte[] buff = new byte[num];
		int off = 0;
		int len = buff.length;
		try {
			while(len>0){
			int temp = ofIO.read(buff, off, len);
			off = off+temp;
			len = len -temp;
			}
		} catch (IOException e) {
			e.printStackTrace();
			fail("Read bytes error");
		}
		return buff;
	}
	
	private byte[] writeBytes(OverflowReadWriter ofIO,int num){
		byte[] buff = new byte[num];
		try {
			ofIO.write(buff);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Write bytes error");
		}
		return buff;
	}
}
