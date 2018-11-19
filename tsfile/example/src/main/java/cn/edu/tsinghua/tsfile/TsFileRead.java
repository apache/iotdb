/**
 * The class is to show how to read TsFile file named "test.ts".
 * The TsFile file "test.ts" is generated from class TsFileWrite1 or class TsFileWrite2, 
 * they generate the same TsFile file by two different ways
 */
package cn.edu.tsinghua.tsfile;

import cn.edu.tsinghua.tsfile.timeseries.basis.TsFile;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

import java.io.IOException;
import java.util.ArrayList;

public class TsFileRead {

	public static void main(String[] args) throws IOException {

		String path = "test.ts";

		// read example : no filter
		TsRandomAccessLocalFileReader input = new TsRandomAccessLocalFileReader(path);
		TsFile readTsFile = new TsFile(input);
		ArrayList<Path> paths = new ArrayList<>();
		paths.add(new Path("device_1.sensor_1"));
		paths.add(new Path("device_1.sensor_2"));
		paths.add(new Path("device_1.sensor_3"));
		QueryDataSet queryDataSet = readTsFile.query(paths, null, null);
		while (queryDataSet.hasNextRecord()) {
			System.out.println(queryDataSet.getNextRecord());
		}
		System.out.println("------------");

		// time filter : 4 <= time < 10
		FilterExpression timeFilter = FilterFactory.and(FilterFactory.gtEq(FilterFactory.timeFilterSeries(), 4L, true),
				FilterFactory.ltEq(FilterFactory.timeFilterSeries(), 10L, false));
		input = new TsRandomAccessLocalFileReader(path);
		readTsFile = new TsFile(input);
		paths = new ArrayList<>();
		paths.add(new Path("device_1.sensor_1"));
		paths.add(new Path("device_1.sensor_2"));
		paths.add(new Path("device_1.sensor_3"));
		queryDataSet = readTsFile.query(paths, timeFilter, null);
		while (queryDataSet.hasNextRecord()) {
			System.out.println(queryDataSet.getNextRecord());
		}
		System.out.println("------------");

		// value filter : device_1.sensor_2 < 20
		FilterExpression valueFilter = FilterFactory
				.ltEq(FilterFactory.intFilterSeries("device_1", "sensor_2", FilterSeriesType.VALUE_FILTER), 20, false);
		input = new TsRandomAccessLocalFileReader(path);
		readTsFile = new TsFile(input);
		paths = new ArrayList<>();
		paths.add(new Path("device_1.sensor_1"));
		paths.add(new Path("device_1.sensor_2"));
		paths.add(new Path("device_1.sensor_3"));
		queryDataSet = readTsFile.query(paths, null, valueFilter);
		while (queryDataSet.hasNextRecord()) {
			System.out.println(queryDataSet.getNextRecord());
		}
		System.out.println("------------");

		// time filter : 4 <= time < 10, value filter : device_1.sensor_3 > 20
		timeFilter = FilterFactory.and(FilterFactory.gtEq(FilterFactory.timeFilterSeries(), 4L, true),
				FilterFactory.ltEq(FilterFactory.timeFilterSeries(), 10L, false));
		valueFilter = FilterFactory
				.gtEq(FilterFactory.intFilterSeries("device_1", "sensor_3", FilterSeriesType.VALUE_FILTER), 20, false);
		input = new TsRandomAccessLocalFileReader(path);
		readTsFile = new TsFile(input);
		paths = new ArrayList<>();
		paths.add(new Path("device_1.sensor_1"));
		paths.add(new Path("device_1.sensor_2"));
		paths.add(new Path("device_1.sensor_3"));
		queryDataSet = readTsFile.query(paths, timeFilter, valueFilter);
		while (queryDataSet.hasNextRecord()) {
			System.out.println(queryDataSet.getNextRecord());
		}
	}

}