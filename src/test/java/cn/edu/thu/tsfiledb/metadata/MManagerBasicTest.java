package cn.edu.thu.tsfiledb.metadata;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.conf.TsfileDBConfig;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.engine.overflow.io.EngineTestHelper;
import cn.edu.thu.tsfiledb.exception.MetadataArgsErrorException;
import cn.edu.thu.tsfiledb.exception.PathErrorException;

public class MManagerBasicTest {

	private static TsfileDBConfig dbconfig = TsfileDBDescriptor.getInstance().getConfig();
	@Before
	public void setUp() throws Exception {
		MManager.getInstance().clear();
		// EngineTestHelper.delete(dbconfig.metadataDir);
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
		assertEquals(manager.pathExist("root.laptop.d1.s1"), false);
		try {
			manager.deletePathFromMTree("root.laptop.d1.s0");
		} catch (PathErrorException | IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		assertEquals(manager.pathExist("root.laptop.d1.s0"), false);
		assertEquals(manager.pathExist("root.laptop.d1"), false);
		assertEquals(manager.pathExist("root.laptop"), false);
		assertEquals(manager.pathExist("root"), true);
		
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
		
		try {
			manager.addPathToMTree("root.laptop.d2.s0","INT32","RLE",new String[0]);
		} catch (PathErrorException | MetadataArgsErrorException | IOException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		}
		try {
			manager.addPathToMTree("root.laptop.d2.s1","INT32","RLE",new String[0]);
		} catch (PathErrorException | MetadataArgsErrorException | IOException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		}
		try {
			manager.setStorageLevelToMTree("root.laptop.d1");
		} catch (PathErrorException | IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		
		try {
			manager.setStorageLevelToMTree("root.laptop");
		} catch (PathErrorException | IOException e) {
			assertEquals(String.format("The storage group %s has been set", "root.laptop.d1"), e.getMessage());
		}
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
			manager.setStorageLevelToMTree("root.laptop");
		} catch (PathErrorException | IOException e) {
			fail(e.getMessage());
		}
		try {
			manager.setStorageLevelToMTree("root.laptop.d2");
		} catch (PathErrorException | IOException e) {
			assertEquals(String.format("The storage group %s has been set", "root.laptop"),e.getMessage());
		}
		/*
		 * check file level
		 */
		assertEquals(manager.pathExist("root.laptop.d2.s1"),true);
		List<Path> paths = new ArrayList<>();
		paths.add(new Path("root.laptop.d2.s1"));
		try {
			manager.checkFileLevel(paths);
		} catch (PathErrorException e) {
			e.printStackTrace();
			fail(e.getMessage());
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
		/*
		 * root d1 s0
		 */
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
		try {
			manager.setStorageLevelToMTree("root.laptop.d1");
		} catch (PathErrorException | IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
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
