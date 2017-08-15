package cn.edu.thu.tsfiledb.metadata;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.thu.tsfiledb.conf.TsfileDBConfig;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.engine.overflow.io.EngineTestHelper;
import cn.edu.thu.tsfiledb.exception.PathErrorException;

public class MTreeTest {

	private static TsfileDBConfig dbconfig = TsfileDBDescriptor.getInstance().getConfig();
	@Before
	public void setUp() throws Exception {
		MManager.getInstance().clear();
	}

	@After
	public void tearDown() throws Exception {
		MManager.getInstance().flushObjectToFile();
		EngineTestHelper.delete(dbconfig.metadataDir);
	}
	
	@Test
	public void testAddLeftNodePath(){
		MTree root = new MTree("root");
		try {
			root.addTimeseriesPath("root.laptop.d1.s1", "INT32", "RLE", new String[0]);
		} catch (PathErrorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			root.addTimeseriesPath("root.laptop.d1.s1.b", "INT32", "RLE", new String[0]);
		} catch (PathErrorException e) {
			assertEquals(String.format("The Node [%s] is left node, the timeseries %s can't be created",
					"s1","root.laptop.d1.s1.b" ), e.getMessage());
		}
	}

	@Test
	public void testAddAndPathExist() {
		MTree root = new MTree("root");
		String path1 = "root";
		assertEquals(root.isPathExist(path1), true);
		assertEquals(root.isPathExist("root.laptop.d1"), false);
		try {
			root.addTimeseriesPath("root.laptop.d1.s1", "INT32", "RLE", new String[0]);
		} catch (PathErrorException e1) {
			fail(e1.getMessage());
		}
		assertEquals(root.isPathExist("root.laptop.d1"), true);
		assertEquals(root.isPathExist("root.laptop"), true);
		assertEquals(root.isPathExist("root.laptop.d1.s2"), false);
		try {
			root.addTimeseriesPath("aa.bb.cc", "INT32", "RLE", new String[0]);
		} catch (PathErrorException e) {
			assertEquals(String.format("Timeseries %s is not right.", "aa.bb.cc"), e.getMessage());
		}
	}
	
	@Test
	public void testAddAndQueryPath() {
		MTree root = new MTree("root");
		try {
			root.addTimeseriesPath("root.a.d0.s0", "INT32", "RLE", new String[0]);
			root.addTimeseriesPath("root.a.d0.s1", "INT32", "RLE", new String[0]);
			root.setStorageGroup("root.a.d0");
			root.addTimeseriesPath("root.a.d1.s0", "INT32", "RLE", new String[0]);
			root.addTimeseriesPath("root.a.d1.s1", "INT32", "RLE", new String[0]);
			root.setStorageGroup("root.a.d1");
			root.addTimeseriesPath("root.a.b.d0.s0", "INT32", "RLE", new String[0]);
			root.setStorageGroup("root.a.b.d0");
		} catch (PathErrorException e1) {
			fail(e1.getMessage());
		}
		try {
			HashMap<String, ArrayList<String>> result = root.getAllPath("root.a.*.s0");
			assertEquals(result.size(), 2);
			assertTrue(result.containsKey("root.a.d1"));
			assertEquals(result.get("root.a.d1").get(0), "root.a.d1.s0");
			assertTrue(result.containsKey("root.a.d0"));
			assertEquals(result.get("root.a.d0").get(0), "root.a.d0.s0");
			
			result = root.getAllPath("root.a.*.*.s0");
			assertTrue(result.containsKey("root.a.b.d0"));
			assertEquals(result.get("root.a.b.d0").get(0), "root.a.b.d0.s0");
		} catch (PathErrorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	@Test
	public void testSetStorageGroup(){
		MTree root = new MTree("root");
		assertEquals(root.isPathExist("root.laptop.d1.s0"), false);
		assertEquals(root.isPathExist("root.laptop.d1.s1"), false);
		assertEquals(root.isPathExist("root.laptop.d2.s0"), false);
		assertEquals(root.isPathExist("root.laptop.d2.s1"), false);
		try {
			root.addTimeseriesPath("root.laptop.d1.s0", "INT32", "RLE", new String[0]);
			root.addTimeseriesPath("root.laptop.d1.s1", "INT32", "RLE", new String[0]);
			root.addTimeseriesPath("root.laptop.d2.s0", "INT32", "RLE", new String[0]);
			root.addTimeseriesPath("root.laptop.d2.s1", "INT32", "RLE", new String[0]);
		} catch (PathErrorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		
		try {
			root.setStorageGroup("root.laptop.d1");
		} catch (PathErrorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			root.setStorageGroup("root.laptop");
		} catch (PathErrorException e) {
			assertEquals(String.format("The storage group %s has been set","root.laptop.d1" ), e.getMessage());
		}
		try {
			root.setStorageGroup("root.laptop.d2");
		} catch (PathErrorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

		try {
			root.deletePath("root.laptop.d1.s0");
		} catch (PathErrorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		assertEquals(root.isPathExist("root.laptop.d1.s0"), false);
		try {
			root.deletePath("root.laptop.d1");
		} catch (PathErrorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		assertEquals(root.isPathExist("root.laptop.d1.s1"), false);
		assertEquals(root.isPathExist("root.laptop.d1"), false);
		assertEquals(root.isPathExist("root.laptop"), true);
		assertEquals(root.isPathExist("root.laptop.d2"), true);
		assertEquals(root.isPathExist("root.laptop.d2.s0"), true);
	}

}
