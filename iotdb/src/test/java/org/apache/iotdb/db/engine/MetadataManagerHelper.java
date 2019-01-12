package org.apache.iotdb.db.engine;

import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.MManager;

/**
 * @author liukun
 *
 */
public class MetadataManagerHelper {
	
	private static MManager mmanager = null;
	
	public static void initMetadata() {
		mmanager = MManager.getInstance();
		mmanager.clear();
		try {
			mmanager.setStorageLevelToMTree("root.vehicle.d0");
			mmanager.setStorageLevelToMTree("root.vehicle.d1");
			mmanager.setStorageLevelToMTree("root.vehicle.d2");

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

			mmanager.addPathToMTree("root.vehicle.d2.s0", "INT32", "RLE", new String[0]);
			mmanager.addPathToMTree("root.vehicle.d2.s1", "INT64", "RLE", new String[0]);
			mmanager.addPathToMTree("root.vehicle.d2.s2", "FLOAT", "RLE", new String[0]);
			mmanager.addPathToMTree("root.vehicle.d2.s3", "DOUBLE", "RLE", new String[0]);
			mmanager.addPathToMTree("root.vehicle.d2.s4", "BOOLEAN", "PLAIN", new String[0]);
			mmanager.addPathToMTree("root.vehicle.d2.s5", "TEXT", "PLAIN", new String[0]);

		} catch (Exception e) {
			throw new RuntimeException("Initialize the metadata manager failed",e);
		}
	}

	public static void initMetadata2() {

		mmanager = MManager.getInstance();
		mmanager.clear();
		try {
			mmanager.setStorageLevelToMTree("root.vehicle");

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

			mmanager.addPathToMTree("root.vehicle.d2.s0", "INT32", "RLE", new String[0]);
			mmanager.addPathToMTree("root.vehicle.d2.s1", "INT64", "RLE", new String[0]);
			mmanager.addPathToMTree("root.vehicle.d2.s2", "FLOAT", "RLE", new String[0]);
			mmanager.addPathToMTree("root.vehicle.d2.s3", "DOUBLE", "RLE", new String[0]);
			mmanager.addPathToMTree("root.vehicle.d2.s4", "BOOLEAN", "PLAIN", new String[0]);
			mmanager.addPathToMTree("root.vehicle.d2.s5", "TEXT", "PLAIN", new String[0]);

		} catch (Exception e) {
			
			throw new RuntimeException("Initialize the metadata manager failed",e);
		}
	}

}
