package org.apache.iotdb.db.metadata;

import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class MTreePerformanceTest {

    private static Logger logger = LoggerFactory.getLogger(MTreePerformanceTest.class);

    private static final int TIMESERIES_NUM = 10000;
    private static final int DEVICE_NUM = 100;

    @Before
    public void setUp() throws Exception {
        EnvironmentUtils.envSetUp();
    }

    @After
    public void tearDown() throws Exception {
        EnvironmentUtils.cleanEnv();
    }


    @Test
    public void testMTreePerformance() throws Exception {
        long startTime = System.currentTimeMillis(), endTime;
        PartialPath[][] paths=new PartialPath[DEVICE_NUM][TIMESERIES_NUM];
        for (int i = 0; i < DEVICE_NUM; i++) {
            for (int j = 0; j < TIMESERIES_NUM; j++) {
                paths[i][j] = new PartialPath("root.t1.v1.d" + i + ".s" + j);
            }
        }
        endTime=System.currentTimeMillis();
        System.out.println("Path creation time cost: "+(endTime-startTime)+"ms");
        MTreeInterface mTreeDisk=testMTreeDisk(paths);
        MTreeInterface mTreeMem=testMTreeMem(paths);


//        System.out.println(ObjectSizeCalculator.getObjectSize(paths));
//        System.out.println(ObjectSizeCalculator.getObjectSize(mTreeMem));
//        System.out.println(ObjectSizeCalculator.getObjectSize(mTreeDisk));
    }

    private MTreeInterface testMTreeMem(PartialPath[][] paths) throws Exception{
        MTreeInterface mTreeMem = new MTree();
        long startTime, endTime;
        mTreeMem.setStorageGroup(new PartialPath("root.t1.v1"));
        startTime=System.currentTimeMillis();
        for (int i = 0; i < DEVICE_NUM; i++) {
            for (int j = 0; j < TIMESERIES_NUM; j++) {
                mTreeMem.createTimeseries(
                        paths[i][j],
                        TSDataType.TEXT,
                        TSEncoding.PLAIN,
                        TSFileDescriptor.getInstance().getConfig().getCompressor(),
                        Collections.emptyMap(),
                        null);
            }
        }
        endTime=System.currentTimeMillis();
        System.out.println("MTreeMem TS creation time cost: "+(endTime-startTime)+"ms");

        startTime=System.currentTimeMillis();
        for (int i = 0; i < DEVICE_NUM; i++) {
            for (int j = 0; j < TIMESERIES_NUM; j++) {
                mTreeMem.isPathExist(paths[i][j]);
            }
        }
        endTime=System.currentTimeMillis();
        System.out.println("MTreeMem TS access time cost: "+(endTime-startTime)+"ms");

        return mTreeMem;
    }

    private MTreeInterface testMTreeDisk(PartialPath[][] paths) throws Exception{
        MTreeInterface mTreeDisk = new MTreeDiskBased();
        long startTime, endTime;
        mTreeDisk.setStorageGroup(new PartialPath("root.t1.v1"));
        startTime=System.currentTimeMillis();
        for (int i = 0; i < DEVICE_NUM; i++) {
            for (int j = 0; j < TIMESERIES_NUM; j++) {
                mTreeDisk.createTimeseries(
                        paths[i][j],
                        TSDataType.TEXT,
                        TSEncoding.PLAIN,
                        TSFileDescriptor.getInstance().getConfig().getCompressor(),
                        Collections.emptyMap(),
                        null);
            }
        }
        endTime=System.currentTimeMillis();
        System.out.println("MTreeDisk TS creation time cost: "+(endTime-startTime)+"ms");

        startTime=System.currentTimeMillis();
        for (int i = 0; i < DEVICE_NUM; i++) {
            for (int j = 0; j < TIMESERIES_NUM; j++) {
                mTreeDisk.isPathExist(paths[i][j]);
            }
        }
        endTime=System.currentTimeMillis();
        System.out.println("MTreeDisk TS access time cost: "+(endTime-startTime)+"ms");

        return mTreeDisk;
    }

}
