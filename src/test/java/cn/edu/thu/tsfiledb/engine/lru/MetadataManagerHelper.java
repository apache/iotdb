package cn.edu.thu.tsfiledb.engine.lru;

import cn.edu.thu.tsfiledb.metadata.MManager;

public class MetadataManagerHelper {
	
	
	private static MManager mmanager =MManager.getInstance();
	
	public static void initMetadata(){
		mmanager.clear();
		try{
		mmanager.addAPathToMTree("root.vehicle.d0.s0", "INT32", "RLE", new String[0]);
		mmanager.addAPathToMTree("root.vehicle.d0.s1", "INT64", "RLE", new String[0]);
		mmanager.addAPathToMTree("root.vehicle.d0.s2", "FLOAT", "RLE", new String[0]);
		mmanager.addAPathToMTree("root.vehicle.d0.s3", "DOUBLE", "RLE", new String[0]);
		mmanager.addAPathToMTree("root.vehicle.d0.s4", "BOOLEAN", "PLAIN", new String[0]);
		mmanager.addAPathToMTree("root.vehicle.d0.s5", "BYTE_ARRAY", "PLAIN", new String[0]);
		
		mmanager.addAPathToMTree("root.vehicle.d1.s0", "INT32", "RLE", new String[0]);
		mmanager.addAPathToMTree("root.vehicle.d1.s1", "INT64", "RLE", new String[0]);
		mmanager.addAPathToMTree("root.vehicle.d1.s2", "FLOAT", "RLE", new String[0]);
		mmanager.addAPathToMTree("root.vehicle.d1.s3", "DOUBLE", "RLE", new String[0]);
		mmanager.addAPathToMTree("root.vehicle.d1.s4", "BOOLEAN", "PLAIN", new String[0]);
		mmanager.addAPathToMTree("root.vehicle.d1.s5", "BYTE_ARRAY", "PLAIN", new String[0]);
		
		mmanager.addAPathToMTree("root.vehicle.d2.s0", "INT32", "RLE", new String[0]);
		mmanager.addAPathToMTree("root.vehicle.d2.s1", "INT64", "RLE", new String[0]);
		mmanager.addAPathToMTree("root.vehicle.d2.s2", "FLOAT", "RLE", new String[0]);
		mmanager.addAPathToMTree("root.vehicle.d2.s3", "DOUBLE", "RLE", new String[0]);
		mmanager.addAPathToMTree("root.vehicle.d2.s4", "BOOLEAN", "PLAIN", new String[0]);
		mmanager.addAPathToMTree("root.vehicle.d2.s5", "BYTE_ARRAY", "PLAIN", new String[0]);

		mmanager.setStorageLevelToMTree("root.vehicle.d0");
		}catch (Exception e) {
			throw new RuntimeException("Initialize the metadata manager failed");
		}
	}
	
	public static void clearMetadata(){
		mmanager.clear();
	}
}
