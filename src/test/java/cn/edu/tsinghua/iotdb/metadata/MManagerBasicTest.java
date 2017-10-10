package cn.edu.tsinghua.iotdb.metadata;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.overflow.io.EngineTestHelper;
import cn.edu.tsinghua.iotdb.exception.MetadataArgsErrorException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.tsfile.timeseries.read.qp.Path;

public class MManagerBasicTest {

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
	public void testAddPathAndExist() {
		
		MManager manager = MManager.getInstance();
		assertEquals(manager.pathExist("root"), true);

		assertEquals(manager.pathExist("root.laptop"), false);
		
		try {
			manager.setStorageLevelToMTree("root.laptop.d1");
		} catch (PathErrorException | IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		
		try {
			manager.setStorageLevelToMTree("root.laptop");
		} catch (PathErrorException | IOException e) {
			assertEquals("The path of root.laptop already exist, it can't be set to the storage group", e.getMessage());
		}
		
		try {
			manager.addPathToMTree("root.laptop.d1.s0","INT32","RLE",new String[0]);
		} catch (PathErrorException | MetadataArgsErrorException | IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		assertEquals(manager.pathExist("root.laptop"), true);
		assertEquals(manager.pathExist("root.laptop.d1"), true);
		assertEquals(manager.pathExist("root.laptop.d1.s0"), true);
		assertEquals(manager.pathExist("root.laptop.d1.s1"), false);
		try {
			manager.addPathToMTree("root.laptop.d1.s1","INT32","RLE",new String[0]);
		} catch (PathErrorException | MetadataArgsErrorException | IOException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		}
		assertEquals(manager.pathExist("root.laptop.d1.s1"), true);
		try {
			manager.deletePathFromMTree("root.laptop.d1.s1");
		} catch (PathErrorException | IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		// just delete s0, and don't delete root.laptop.d1??
		// delete storage group or not
		assertEquals(manager.pathExist("root.laptop.d1.s1"), false);
		try {
			manager.deletePathFromMTree("root.laptop.d1.s0");
		} catch (PathErrorException | IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		assertEquals(manager.pathExist("root.laptop.d1.s0"), false);
		assertEquals(manager.pathExist("root.laptop.d1"), true);
		assertEquals(manager.pathExist("root.laptop"), true);
		assertEquals(manager.pathExist("root"), true);
		
		// can't delete the storage group
		
		// try {
		// manager.setStorageLevelToMTree("root.laptop");
		// } catch (PathErrorException | IOException e) {
		// fail(e.getMessage());
		// }
		try {
			manager.addPathToMTree("root.laptop.d1.s1","INT32","RLE",new String[0]);
		} catch (PathErrorException | MetadataArgsErrorException | IOException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		}
		
		try {
			manager.addPathToMTree("root.laptop.d1.s0","INT32","RLE",new String[0]);
		} catch (PathErrorException | MetadataArgsErrorException | IOException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		}

		assertEquals(false, manager.pathExist("root.laptop.d2"));
		assertEquals(false, manager.checkFileNameByPath("root.laptop.d2"));
	
		try {
			manager.deletePathFromMTree("root.laptop.d1.s0");
		} catch (PathErrorException | IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			manager.deletePathFromMTree("root.laptop.d1.s1");
		} catch (PathErrorException | IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		
		try {
			manager.setStorageLevelToMTree("root.laptop.d2");
		} catch (PathErrorException | IOException e) {
			assertEquals(String.format("The path of %s already exist, it can't be set to the storage group", "root.laptop.d2"),e.getMessage());
		}
		/*
		 * check file level
		 */
		assertEquals(manager.pathExist("root.laptop.d2.s1"),false);
		List<Path> paths = new ArrayList<>();
		paths.add(new Path("root.laptop.d2.s1"));
		try {
			manager.checkFileLevel(paths);
		} catch (PathErrorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		
		try {
			manager.addPathToMTree("root.laptop.d2.s1","INT32","RLE",new String[0]);
		} catch (PathErrorException | MetadataArgsErrorException | IOException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		}
		
		try {
			manager.addPathToMTree("root.laptop.d2.s0","INT32","RLE",new String[0]);
		} catch (PathErrorException | MetadataArgsErrorException | IOException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		}
		
		try {
			manager.deletePathFromMTree("root.laptop.d2.s0");
		} catch (PathErrorException | IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			manager.deletePathFromMTree("root.laptop.d2.s1");
		} catch (PathErrorException | IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		
		try {
			manager.addPathToMTree("root.laptop.d1.s0","INT32","RLE",new String[0]);
		} catch (PathErrorException | MetadataArgsErrorException | IOException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		}
		
		try {
			manager.addPathToMTree("root.laptop.d1.s1","INT32","RLE",new String[0]);
		} catch (PathErrorException | MetadataArgsErrorException | IOException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		}
	
		paths = new ArrayList<>();
		paths.add(new Path("root.laptop.d1.s0"));
		try {
			manager.checkFileLevel(paths);
		} catch (PathErrorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		
		try {
			manager.addPathToMTree("root.laptop.d1.s2","INT32","RLE",new String[0]);
		} catch (PathErrorException | MetadataArgsErrorException | IOException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		}
		paths = new ArrayList<>();
		paths.add(new Path("root.laptop.d1.s2"));
		try {
			manager.checkFileLevel(paths);
		} catch (PathErrorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		
		try {
			manager.addPathToMTree("root.laptop.d1.s3","INT32","RLE",new String[0]);
		} catch (PathErrorException | MetadataArgsErrorException | IOException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		}
		paths = new ArrayList<>();
		paths.add(new Path("root.laptop.d1.s3"));
		try {
			manager.checkFileLevel(paths);
		} catch (PathErrorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
