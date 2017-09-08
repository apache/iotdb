package cn.edu.thu.tsfiledb.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.After;
import org.junit.Before;

import cn.edu.thu.tsfiledb.conf.TsfileDBConfig;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.engine.overflow.io.EngineTestHelper;
import cn.edu.thu.tsfiledb.exception.PathErrorException;

public class MManagerAdvancedTest {

	private static MManager mmanager = null;
	private TsfileDBConfig dbconfig = TsfileDBDescriptor.getInstance().getConfig();

	@Before
	public void setUp() throws Exception {
		
		mmanager = MManager.getInstance();
		mmanager.clear();

		mmanager.addPathToMTree("root.vehicle.d0.s0", "INT32", "RLE", new String[0]);
		mmanager.addPathToMTree("root.vehicle.d0.s1", "INT64", "RLE", new String[0]);
		mmanager.addPathToMTree("root.vehicle.d0.s2", "FLOAT", "RLE", new String[0]);
		mmanager.addPathToMTree("root.vehicle.d0.s3", "DOUBLE", "RLE", new String[0]);
		mmanager.addPathToMTree("root.vehicle.d0.s4", "BOOLEAN", "PLAIN", new String[0]);
		mmanager.addPathToMTree("root.vehicle.d0.s5", "TEXT", "PLAIN", new String[0]);

		mmanager.addPathToMTree("root.vehicle.d1.s0", "INT32", "RLE", new String[0]);
		mmanager.addPathToMTree("root.vehicle.d1.s1", "INT64", "RLE", new String[0]);
		mmanager.addPathToMTree("root.vehicle.d1.s2", "FLOAT", "RLE", new String[0]);
		mmanager.addPathToMTree("root.vehicle.d1.s3", "DOUBLE", "RLE", new String[0]);
		mmanager.addPathToMTree("root.vehicle.d1.s4", "BOOLEAN", "PLAIN", new String[0]);
		mmanager.addPathToMTree("root.vehicle.d1.s5", "TEXT", "PLAIN", new String[0]);

		mmanager.setStorageLevelToMTree("root.vehicle.d1");
		mmanager.setStorageLevelToMTree("root.vehicle.d0");
		
	}

	@After
	public void tearDown() throws Exception {
		MManager.getInstance().flushObjectToFile();
		EngineTestHelper.delete(dbconfig.dataDir);
	}

	@org.junit.Test
	public void test() {

		try {
			// test file name
			List<String> fileNames = mmanager.getAllFileNames();
			assertEquals(2, fileNames.size());
			if (fileNames.get(0).equals("root.vehicle.d0")) {
				assertEquals(fileNames.get(1), "root.vehicle.d1");
			} else {
				assertEquals(fileNames.get(1), "root.vehicle.d0");
			}
			// test filename by path
			assertEquals("root.vehicle.d0", mmanager.getFileNameByPath("root.vehicle.d0.s1"));
			HashMap<String, ArrayList<String>> map = mmanager.getAllPathGroupByFileName("root.vehicle.d1.*");
			assertEquals(1, map.keySet().size());
			assertEquals(6, map.get("root.vehicle.d1").size());
			ArrayList<String> paths = mmanager.getPaths("root.vehicle.d0");
			assertEquals(6, paths.size());
			paths = mmanager.getPaths("root.vehicle.d2");
			assertEquals(0, paths.size());
		} catch (PathErrorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
