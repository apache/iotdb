package org.apache.iotdb.tsfile.hadoop;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.iotdb.tsfile.common.utils.ITsRandomAccessFileReader;
import org.apache.iotdb.tsfile.timeseries.basis.TsFile;
import org.apache.iotdb.tsfile.read.TsRandomAccessLocalFileReader;

public class TSFHadoopTest {

	private TSFInputFormat inputformat = null;

	private String tsfilePath = "tsfile";

	@Before
	public void setUp() throws Exception {

		TsFileTestHelper.deleteTsFile(tsfilePath);
		inputformat = new TSFInputFormat();
	}

	@After
	public void tearDown() throws Exception {

		TsFileTestHelper.deleteTsFile(tsfilePath);
	}

	@Test
	public void staticMethodTest() {
		Job job = null;
		try {
			job = Job.getInstance();
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		//
		// columns
		//
		String[] value = { "s1", "s2", "s3" };
		try {
			TSFInputFormat.setReadMeasurementIds(job, value);
			String[] getValue = (String[])TSFInputFormat.getReadMeasurementIds(job.getConfiguration()).toArray();
			assertArrayEquals(value, getValue);
		} catch (TSFHadoopException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		//
		// deviceid
		//
		TSFInputFormat.setReadDeltaObjectId(job, true);
		assertEquals(true, TSFInputFormat.getReadDeltaObject(job.getConfiguration()));

		//
		// time
		//

		TSFInputFormat.setReadTime(job, true);
		assertEquals(true, TSFInputFormat.getReadTime(job.getConfiguration()));

		//
		// filter
		//
		TSFInputFormat.setHasFilter(job, true);
		assertEquals(true, TSFInputFormat.getHasFilter(job.getConfiguration()));

		String filterType = "singleFilter";
		TSFInputFormat.setFilterType(job, filterType);
		assertEquals(filterType, TSFInputFormat.getFilterType(job.getConfiguration()));

		String filterExpr = "s1>100";
		TSFInputFormat.setFilterExp(job, filterExpr);
		assertEquals(filterExpr, TSFInputFormat.getFilterExp(job.getConfiguration()));
	}

	@Test
	public void InputFormatTest() {

		//
		// test getinputsplit method
		//
		TsFileTestHelper.writeTsFile(tsfilePath);
		try {
			Job job = Job.getInstance();
			// set input path to the job
			TSFInputFormat.setInputPaths(job, tsfilePath);
			List<InputSplit> inputSplits = inputformat.getSplits(job);
			ITsRandomAccessFileReader reader = new TsRandomAccessLocalFileReader(tsfilePath);
			TsFile tsFile = new TsFile(reader);
			System.out.println(tsFile.getDeltaObjectRowGroupCount());
			//assertEquals(tsFile.getRowGroupPosList().size(), inputSplits.size());
			for (InputSplit inputSplit : inputSplits) {
				System.out.println(inputSplit);
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void RecordReaderTest() {
		TsFileTestHelper.writeTsFile(tsfilePath);
		try {
			Job job = Job.getInstance();
			// set input path to the job
			TSFInputFormat.setInputPaths(job, tsfilePath);
			String[] devices = {"root.car.d1"};
			TSFInputFormat.setReadDeltaObjectIds(job, devices);
			String[] sensors = { "s1", "s2", "s3", "s4", "s5", "s6"};
			TSFInputFormat.setReadMeasurementIds(job, sensors);
			TSFInputFormat.setReadDeltaObjectId(job, false);
			TSFInputFormat.setReadTime(job, false);
			List<InputSplit> inputSplits = inputformat.getSplits(job);
			ITsRandomAccessFileReader reader = new TsRandomAccessLocalFileReader(tsfilePath);
			TsFile tsFile = new TsFile(reader);
			System.out.println(tsFile.getDeltaObjectRowGroupCount());
			//assertEquals(tsFile.getRowGroupPosList().size(), inputSplits.size());
			for (InputSplit inputSplit : inputSplits) {
				System.out.println(inputSplit);
			}
			reader.close();
			// read one split
			TSFRecordReader recordReader = new TSFRecordReader();
			TaskAttemptContextImpl attemptContextImpl = new TaskAttemptContextImpl(job.getConfiguration(),
					new TaskAttemptID());
			recordReader.initialize(inputSplits.get(0), attemptContextImpl);
			while (recordReader.nextKeyValue()) {
				assertEquals(recordReader.getCurrentValue().get().length, sensors.length);
				for (Writable writable : recordReader.getCurrentValue().get()) {
					if (writable instanceof IntWritable) {
						assertEquals(writable.toString(), "1");
					} else if (writable instanceof LongWritable) {
						assertEquals(writable.toString(), "1");
					} else if (writable instanceof FloatWritable) {
						assertEquals(writable.toString(), "0.1");
					} else if (writable instanceof DoubleWritable) {
						assertEquals(writable.toString(), "0.1");
					} else if (writable instanceof BooleanWritable) {
						assertEquals(writable.toString(), "true");
					} else if (writable instanceof Text) {
						assertEquals(writable.toString(), "tsfile");
					} else {
						fail(String.format("Not support type %s", writable.getClass().getName()));
					}
				}
			}
			recordReader.close();
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (TSFHadoopException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
