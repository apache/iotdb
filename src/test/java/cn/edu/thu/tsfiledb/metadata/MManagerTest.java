package cn.edu.thu.tsfiledb.metadata;

import static org.junit.Assert.fail;

import java.awt.geom.Path2D;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.exception.MetadataArgsErrorException;
import cn.edu.thu.tsfiledb.exception.PathErrorException;




public class MManagerTest {
	
	private MManager mManager = MManager.getInstance();
	
	@Before
	public void before(){
		mManager.clear();
		try {
			mManager.addAPathToMTree("root.vehicle.d1.s1", "INT32", "RLE", new String[0]);
			mManager.addAPathToMTree("root.vehicle.d1.s2", "INT32", "RLE", new String[0]);
			mManager.addAPathToMTree("root.vehicle.d1.s3", "INT32", "RLE", new String[0]);
			mManager.addAPathToMTree("root.vehicle.d1.s4", "INT32", "RLE", new String[0]);
			mManager.addAPathToMTree("root.vehicle.d2.s1", "INT32", "RLE", new String[0]);
			mManager.addAPathToMTree("root.vehicle.d2.s2", "FLOAT", "TS_2DIFF", new String[0]);
			mManager.addAPathToMTree("root.vehicle.d2.s3", "DOUBLE", "RLE", new String[0]);
			mManager.addAPathToMTree("root.vehicle.d2.s4", "INT64", "RLE", new String[0]);
			mManager.addAPathToMTree("root.laptop.d1.s1", "INT32", "RLE", new String[0]);
			mManager.addAPathToMTree("root.laptop.d1.s2", "INT32", "RLE", new String[0]);
			mManager.addAPathToMTree("root.laptop.d1.s3", "INT32", "RLE", new String[0]);
			mManager.addAPathToMTree("root.laptop.d1.s4", "INT32", "RLE", new String[0]);
			mManager.addAPathToMTree("root.laptop.d2.s1", "INT32", "RLE", new String[0]);
			mManager.addAPathToMTree("root.laptop.d2.s2", "FLOAT", "TS_2DIFF", new String[0]);
			mManager.addAPathToMTree("root.laptop.d2.s3", "DOUBLE", "RLE", new String[0]);
			
			mManager.setStorageLevelToMTree("root.vehicle.d1");
			mManager.setStorageLevelToMTree("root.laptop");
		} catch (Exception e) {
			e.printStackTrace();
			fail("Exception when executing...");
		} 
	}
	
	@After
	public void after(){
		mManager.deleteLogAndDataFiles();
	}
	
	@Test
	public void deletePathFromMTree() {
		try {
			Assert.assertEquals(true, mManager.pathExist("root.vehicle.d2.s4"));
			mManager.deletePathFromMTree("root.vehicle.d2.s4");
			Assert.assertEquals(false, mManager.pathExist("root.vehicle.d2.s4"));
			
		} catch (PathErrorException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void testQueryInMTree(){
		
		String deltaObjectType;
		try {
			deltaObjectType = mManager.getDeltaObjectTypeByPath("root.vehicle.d2.s4");
			Assert.assertEquals("root.vehicle", deltaObjectType);
			
			TSDataType type = mManager.getSeriesType("root.vehicle.d2.s4");
			Assert.assertEquals(TSDataType.INT64, type);
			
			Map<String, List<ColumnSchema>> ret = mManager.getSchemaForAllType();
			Assert.assertEquals(4, ret.get("vehicle").size());
			mManager.deletePathFromMTree("root.vehicle.d2.s4");
			ret = mManager.getSchemaForAllType();
			Assert.assertEquals(4, ret.get("vehicle").size());
			mManager.deletePathFromMTree("root.vehicle.d1.s4");
			ret = mManager.getSchemaForAllType();
			Assert.assertEquals(3, ret.get("vehicle").size());
			mManager.deletePathFromMTree("root.vehicle.d1");
			
			Metadata meta = mManager.getMetadata();
			Assert.assertEquals(1, meta.getDeltaObjectsForOneType("vehicle").size());
			
			
		} catch (PathErrorException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	@Test
	public void testQueryInMTree2(){
		
		try {
			List<ColumnSchema> ret = mManager.getSchemaForOneType("root.vehicle");
			Assert.assertEquals(4, ret.size());
			
			int cnt = mManager.getFileCountForOneType("root.laptop");
			Assert.assertEquals(1, cnt);
			
			String fileName = mManager.getFileNameByPath("root.laptop.d1.s1");
			Assert.assertEquals("root.laptop", fileName);
			
			HashMap<String, ArrayList<String>> ret2 = mManager.getAllPathGroupByFileName("root.vehicle");
			Assert.assertEquals(2, ret2.keySet().size());
			
			List<String> paths = mManager.getPaths("root.vehicle.*.s1");
			Assert.assertEquals(2, paths.size());
			Assert.assertEquals("root.vehicle.d1.s1", paths.get(0));
			
			boolean ret3 = mManager.pathExist("root.vehiccc.d1.s2");
			Assert.assertEquals(false, ret3);
			
			ColumnSchema cs = mManager.getSchemaForOnePath("root.vehicle.d1.s1");
			Assert.assertEquals("s1", cs.name);
			Assert.assertEquals(TSDataType.INT32, cs.dataType);
			
			List<Path> paths2 = new ArrayList<Path>();
			paths2.add(new Path("root.vehicle.d1.s1"));
			boolean ret4 = mManager.checkFileLevel(paths2);
			Assert.assertEquals(true, ret4);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testPTree(){
		try {
			mManager.addAPTree("region");
			mManager.addAPathToPTree("region.beijing");
			mManager.addAPathToPTree("region.shanghai");
			mManager.linkMNodeToPTree("region.beijing", "root.vehicle.d1.s1");
			mManager.linkMNodeToPTree("region.beijing", "root.vehicle.d1.s2");
			mManager.linkMNodeToPTree("region.beijing", "root.vehicle.d1.s3");
			Assert.assertEquals(3, mManager.getAllPathGroupByFileName("region.beijing").get("root.vehicle.d1").size());
			mManager.unlinkMNodeFromPTree("region.beijing", "root.vehicle.d1.s3");
			Assert.assertEquals(2, mManager.getAllPathGroupByFileName("region.beijing").get("root.vehicle.d1").size());
			mManager.unlinkMNodeFromPTree("region.beijing", "root.vehicle.d1.s2");
			mManager.unlinkMNodeFromPTree("region.beijing", "root.vehicle.d1.s1");
			Assert.assertEquals(false, mManager.getAllPathGroupByFileName("region.beijing").containsKey("root.vehicle.d1"));
			mManager.linkMNodeToPTree("region.shanghai", "root.vehicle.d1.s1");
			mManager.linkMNodeToPTree("region.shanghai", "root.vehicle.d1.s2");
			Assert.assertEquals(true, mManager.getAllPathGroupByFileName("region.shanghai").containsKey("root.vehicle.d1"));
			
		} catch (PathErrorException | IOException | MetadataArgsErrorException e) {
			e.printStackTrace();
		}
		
		try {
			mManager.deletePathFromPTree("region.shanghai");
			mManager.getAllPathGroupByFileName("region.shanghai").containsKey("root.vehicle.d1");
			fail("region.shanghai has been deleted");
		} catch (PathErrorException | IOException e) {
			Assert.assertEquals(true, true);
		}
		
	}
	
	@Test
	public void testFlush(){
		try {
			mManager.flushObjectToFile();
			mManager.deleteLogAndDataFiles();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void testLink(){
		try {
			mManager.addAPTree("region");
			mManager.addAPathToPTree("region.beijing");
			mManager.linkMNodeToPTree("region.beijing", "root.vehicle.d1");
			mManager.linkMNodeToPTree("region.beijing", "root.vehicle.d1.s1");
			mManager.unlinkMNodeFromPTree("region.beijing", "root.vehicle.d1.s1");
			mManager.linkMNodeToPTree("region.beijing", "root.vehicle.d1.s2");
			mManager.unlinkMNodeFromPTree("region.beijing", "root.vehicle.d1");
			Assert.assertEquals(0, mManager.getAllPathGroupByFileName("region.beijing").size());
			
		} catch (IOException | MetadataArgsErrorException | PathErrorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}

	
	@Test
	public void testPTree2(){
		try {
			mManager.addAPTree("region");
		} catch (IOException | MetadataArgsErrorException e) {
			e.printStackTrace();
		}
		try {
			mManager.addAPathToPTree("city.beijing");
			fail("city has not been added into PTree");
		} catch (PathErrorException | IOException | MetadataArgsErrorException e) {
			Assert.assertEquals(true, true);
		}
	}
}













