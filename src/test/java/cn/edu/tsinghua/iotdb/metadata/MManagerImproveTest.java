package cn.edu.tsinghua.iotdb.metadata;

import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MManagerImproveTest {

    private static MManager mManager = null;
    private static final int TIMESERIES_NUM = 1000;
    private static final int DEVICE_NUM = 10;

    @Before
    public void setUp() throws Exception {
        mManager = MManager.getInstance();
        mManager.setStorageLevelToMTree("root.t1.v2");

        for(int j = 0;j < DEVICE_NUM;j++) {
            for (int i = 0; i < TIMESERIES_NUM; i++) {
                String p = new StringBuilder().append("root.t1.v2.d").append(j).append(".s").append(i).toString();
                mManager.addPathToMTree(p, "TEXT", "RLE", new String[0]);
            }
        }

        mManager.flushObjectToFile();
    }

    @After
    public void after() throws IOException {
        EnvironmentUtils.cleanEnv();
    }

    @Test
    public void checkSetUp(){
        mManager = MManager.getInstance();

        assertEquals(true, mManager.pathExist("root.t1.v2.d3.s5"));
        assertEquals(false, mManager.pathExist("root.t1.v2.d9.s" + TIMESERIES_NUM));
        assertEquals(false, mManager.pathExist("root.t10"));
    }

    @Test
    public void analyseTimeCost() throws PathErrorException, ProcessorException {
        mManager = MManager.getInstance();

        long startTime, endTime;
        long string_combine, path_exist, list_init, check_filelevel, get_seriestype;
        string_combine = path_exist = list_init = check_filelevel = get_seriestype = 0;

        String deltaObject = "root.t1.v2.d3";
        String measurement = "s5";

        startTime = System.currentTimeMillis();
        for(int i = 0;i < 100000;i++) {
            String path = deltaObject + "." + measurement;
        }
        endTime = System.currentTimeMillis();
        string_combine += endTime - startTime;
        String path = deltaObject + "." + measurement;

        startTime = System.currentTimeMillis();
        for(int i = 0;i < 100000;i++) {
            assertEquals(true, mManager.pathExist(path));
        }
        endTime = System.currentTimeMillis();
        path_exist += endTime - startTime;

        startTime = System.currentTimeMillis();
        for(int i = 0;i < 100000;i++) {
            List<Path> paths = new ArrayList<>();
            paths.add(new Path(path));
        }
        endTime = System.currentTimeMillis();
        list_init += endTime - startTime;
        List<Path> paths = new ArrayList<>();
        paths.add(new Path(path));

        startTime = System.currentTimeMillis();
        for(int i = 0;i < 100000;i++) {
            assertEquals(true, mManager.checkFileLevel(paths));
        }
        endTime = System.currentTimeMillis();
        check_filelevel += endTime - startTime;

        startTime = System.currentTimeMillis();
        for(int i = 0;i < 100000;i++) {
            TSDataType dataType = mManager.getSeriesType(path);
            assertEquals(TSDataType.TEXT, dataType);
        }
        endTime = System.currentTimeMillis();
        get_seriestype += endTime - startTime;

        System.out.println("string combine:\t" + string_combine);
        System.out.println("path exist:\t" + path_exist);
        System.out.println("list init:\t" + list_init);
        System.out.println("check file level:\t" + check_filelevel);
        System.out.println("get series type:\t" + get_seriestype);
    }

    public void doOriginTest(String deltaObject, List<String> measurementList)
            throws PathErrorException, ProcessorException {
        for(String measurement : measurementList){
            String path = deltaObject + "." + measurement;
            assertEquals(true, mManager.pathExist(path));
            List<Path> paths = new ArrayList<>();
            paths.add(new Path(path));
            assertEquals(true, mManager.checkFileLevel(paths));
            TSDataType dataType = mManager.getSeriesType(path);
            assertEquals(TSDataType.TEXT, dataType);
        }
    }

    public void doPathLoopOnceTest(String deltaObject, List<String> measurementList)
            throws PathErrorException, ProcessorException {
        for(String measurement : measurementList){
            String path = deltaObject + "." + measurement;
            List<Path> paths = new ArrayList<>();
            paths.add(new Path(path));
            assertEquals(true, mManager.checkFileLevel(paths));
            TSDataType dataType = mManager.getSeriesTypeWithCheck(path);
            assertEquals(TSDataType.TEXT, dataType);
        }
    }

    public void doDealDeltaObjectOnceTest(String deltaObject, List<String> measurementList)
            throws PathErrorException, ProcessorException {
        boolean isFileLevelChecked;
        List<Path> tempList = new ArrayList<>();
        tempList.add(new Path(deltaObject));
        try{
            isFileLevelChecked = mManager.checkFileLevel(tempList);
        } catch (PathErrorException e){
            isFileLevelChecked = false;
        }
        MNode node = mManager.getNodeByPath(deltaObject);

        for(String measurement : measurementList){
            assertEquals(true, mManager.pathExist(node, measurement));
            List<Path> paths = new ArrayList<>();
            paths.add(new Path(measurement));
            if(!isFileLevelChecked)isFileLevelChecked = mManager.checkFileLevel(node, paths);
            assertEquals(true, isFileLevelChecked);
            TSDataType dataType = mManager.getSeriesType(node, measurement);
            assertEquals(TSDataType.TEXT, dataType);
        }
    }

    public void doRemoveListTest(String deltaObject, List<String> measurementList)
            throws PathErrorException, ProcessorException {
        for(String measurement : measurementList){
            String path = deltaObject + "." + measurement;
            assertEquals(true, mManager.pathExist(path));
            assertEquals(true, mManager.checkFileLevel(path));
            TSDataType dataType = mManager.getSeriesType(path);
            assertEquals(TSDataType.TEXT, dataType);
        }
    }

    public void doAllImproveTest(String deltaObject, List<String> measurementList)
            throws PathErrorException, ProcessorException {
        boolean isFileLevelChecked;
        try{
            isFileLevelChecked = mManager.checkFileLevel(deltaObject);
        } catch (PathErrorException e){
            isFileLevelChecked = false;
        }
        MNode node = mManager.getNodeByPathWithCheck(deltaObject);

        for(String measurement : measurementList){
            if(!isFileLevelChecked)isFileLevelChecked = mManager.checkFileLevelWithCheck(node, measurement);
            assertEquals(true, isFileLevelChecked);
            TSDataType dataType = mManager.getSeriesTypeWithCheck(node, measurement);
            assertEquals(TSDataType.TEXT, dataType);
        }
    }

    public void doCacheTest(String deltaObject, List<String> measurementList)
            throws PathErrorException, ProcessorException {
        MNode node = mManager.getNodeByDeltaObjectIDFromCache(deltaObject);
        for (int i = 0; i < measurementList.size(); i++) {
            assertEquals(true, node.hasChild(measurementList.get(i)));
            MNode measurementNode = node.getChild(measurementList.get(i));
            assertEquals(true, measurementNode.isLeaf());
            TSDataType dataType = measurementNode.getSchema().dataType;
            assertEquals(TSDataType.TEXT, dataType);
        }
    }

    @Test
    public void improveTest() throws PathErrorException, ProcessorException {
        mManager = MManager.getInstance();

        long startTime, endTime;
        String[] deltaObjectList = new String[DEVICE_NUM];
        for(int i = 0;i < DEVICE_NUM;i++){
            deltaObjectList[i] = "root.t1.v2.d" + i;
        }
        List<String> measurementList = new ArrayList<>();
        for(int i = 0;i < TIMESERIES_NUM;i++){
            measurementList.add("s" + i);
        }

        startTime = System.currentTimeMillis();
        for(String deltaObject : deltaObjectList) {
            doOriginTest(deltaObject, measurementList);
        }
        endTime = System.currentTimeMillis();
        System.out.println("origin:\t" + (endTime - startTime));

        startTime = System.currentTimeMillis();
        for(String deltaObject : deltaObjectList) {
            doPathLoopOnceTest(deltaObject, measurementList);
        }
        endTime = System.currentTimeMillis();
        System.out.println("path loop once:\t" + (endTime - startTime));

        startTime = System.currentTimeMillis();
        for(String deltaObject : deltaObjectList) {
            doDealDeltaObjectOnceTest(deltaObject, measurementList);
        }
        endTime = System.currentTimeMillis();
        System.out.println("deal deltaObject once:\t" + (endTime - startTime));

        startTime = System.currentTimeMillis();
        for(String deltaObject : deltaObjectList) {
            doRemoveListTest(deltaObject, measurementList);
        }
        endTime = System.currentTimeMillis();
        System.out.println("remove list:\t" + (endTime - startTime));

        startTime = System.currentTimeMillis();
        for(String deltaObject : deltaObjectList) {
            doAllImproveTest(deltaObject, measurementList);
        }
        endTime = System.currentTimeMillis();
        System.out.println("improve all:\t" + (endTime - startTime));

        startTime = System.currentTimeMillis();
        for(String deltaObject : deltaObjectList) {
            doCacheTest(deltaObject, measurementList);
        }
        endTime = System.currentTimeMillis();
        System.out.println("add cache:\t" + (endTime - startTime));
    }

    @After
    public void tearDown() throws Exception {
        EnvironmentUtils.cleanEnv();
    }

}
