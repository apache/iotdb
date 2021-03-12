package org.apache.iotdb.tsfile;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class WritePerformanceTest {
	static final int TIMESERIES_NUM = 400;

	public static void main(String[] args) throws Exception {
		int rowNum = Integer.valueOf(args[0]);
		String path = "/data/test.tsfile";
		File f = FSFactoryProducer.getFSFactory().getFile(path);
		if (f.exists() && !f.delete()) {
			throw new RuntimeException("can not delete " + f.getAbsolutePath());
		}
		String device = "device";
		Schema schema = new Schema();
		List<MeasurementSchema> schemas = new ArrayList<>();
		for (int j = 0; j < TIMESERIES_NUM; ++j) {
			MeasurementSchema measurementSchema = new MeasurementSchema("s" + j, TSDataType.DOUBLE, TSEncoding.RLE);
			schemas.add(measurementSchema);
			schema.registerTimeseries(new Path(device, "s" + j), new MeasurementSchema("s" + j, TSDataType.DOUBLE, TSEncoding.RLE));
		}

		try (TsFileWriter tsFileWriter = new TsFileWriter(f, schema)) {
			long startTime = System.currentTimeMillis();
			// construct the tablet
			Tablet tablet = new Tablet(device, schemas);

			long[] timestamps = tablet.timestamps;
			Object[] values = tablet.values;

			long timestamp = 1;
			Random random = new Random();

			for (int r = 0; r < rowNum; r++) {
				int row = tablet.rowSize++;
				timestamps[row] = timestamp++;
				double value = random.nextDouble();
				for (int i = 0; i < TIMESERIES_NUM; i++) {
					double[] sensor = (double[]) values[i];
					sensor[row] = value;
				}
				// write Tablet to TsFile
				if (tablet.rowSize == tablet.getMaxRowNumber()) {
					tsFileWriter.write(tablet);
					tablet.reset();
				}
			}
			// write Tablet to TsFile
			if (tablet.rowSize != 0) {
				tsFileWriter.write(tablet);
				tablet.reset();
			}

		}
	}

}
