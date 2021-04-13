package org.apache.iotdb.db.layoutoptimize.diskevaluator;

public class DiskEvaluator {
	public static class DiskEvaluatorHolder {
		public static DiskEvaluator INSTANCE = new DiskEvaluator();

		public DiskEvaluator getInstance() {
			return INSTANCE;
		}
	}
}
