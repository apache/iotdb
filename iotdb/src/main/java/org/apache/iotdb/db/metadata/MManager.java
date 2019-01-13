/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.metadata;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.MetadataArgsErrorException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.utils.RandomDeleteCache;
import org.apache.iotdb.tsfile.exception.cache.CacheException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.db.exception.MetadataArgsErrorException;
import org.apache.iotdb.db.exception.PathErrorException;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class takes the responsibility of serialization of all the metadata info and persistent it into files. This
 * class contains all the interfaces to modify the metadata for delta system. All the operations will be write into the
 * logs temporary in case the downtime of the delta system.
 *
 * @author Jinrui Zhang
 */
public class MManager {
    // private static MManager manager = new MManager();
    private static final String ROOT_NAME = MetadataConstant.ROOT;
    // the lock for read/write
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    // The file storing the serialize info for metadata
    private String datafilePath;
    // the log file seriesPath
    private String logFilePath;
    private MGraph mGraph;
    private BufferedWriter logWriter;
    private boolean writeToLog;
    private String metadataDirPath;

    private RandomDeleteCache<String, PathCheckRet> checkAndGetDataTypeCache;
    private RandomDeleteCache<String, MNode> mNodeCache;

    private static class MManagerHolder {
        private static final MManager INSTANCE = new MManager();
    }

    public static MManager getInstance() {
        return MManagerHolder.INSTANCE;
    }

    private MManager() {
        metadataDirPath = IoTDBDescriptor.getInstance().getConfig().metadataDir;
        if (metadataDirPath.length() > 0
                && metadataDirPath.charAt(metadataDirPath.length() - 1) != File.separatorChar) {
            metadataDirPath = metadataDirPath + File.separatorChar;
        }
        File metadataDir = new File(metadataDirPath);
        if (!metadataDir.exists()) {
            metadataDir.mkdirs();
        }
        datafilePath = metadataDirPath + MetadataConstant.METADATA_OBJ;
        logFilePath = metadataDirPath + MetadataConstant.METADATA_LOG;
        writeToLog = false;

        int cacheSize = IoTDBDescriptor.getInstance().getConfig().mManagerCacheSize;
        checkAndGetDataTypeCache = new RandomDeleteCache<String, PathCheckRet>(cacheSize) {
            @Override
            public void beforeRemove(PathCheckRet object) throws CacheException {

            }

            @Override
            public PathCheckRet loadObjectByKey(String key) throws CacheException {
                return loadPathToCache(key);
            }
        };

        mNodeCache = new RandomDeleteCache<String, MNode>(cacheSize) {
            @Override
            public void beforeRemove(MNode object) throws CacheException {
            }

            @Override
            public MNode loadObjectByKey(String key) throws CacheException {
                try {
                    return getNodeByPathWithCheck(key);
                } catch (PathErrorException e) {
                    throw new CacheException(e);
                }
            }
        };

        init();
    }

    private void init() {

        lock.writeLock().lock();
        File dataFile = new File(datafilePath);
        File logFile = new File(logFilePath);
        try {
            try {
                if (dataFile.exists()) {
                    // init the metadata from the serialized file
                    FileInputStream fis = new FileInputStream(dataFile);
                    ObjectInputStream ois = new ObjectInputStream(fis);
                    mGraph = (MGraph) ois.readObject();
                    ois.close();
                    fis.close();
                    dataFile.delete();
                } else {
                    // init the metadata from the operation log
                    mGraph = new MGraph(ROOT_NAME);
                    if (logFile.exists()) {
                        FileReader fr = new FileReader(logFile);
                        BufferedReader br = new BufferedReader(fr);
                        String cmd;
                        while ((cmd = br.readLine()) != null) {
                            operation(cmd);
                        }
                        br.close();
                    }
                }
                FileWriter fw = new FileWriter(logFile, true);
                logWriter = new BufferedWriter(fw);
                writeToLog = true;
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void clear() {
        lock.writeLock().lock();
        try {
            this.mGraph = new MGraph(ROOT_NAME);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void operation(String cmd) throws PathErrorException, IOException, MetadataArgsErrorException {

        String args[] = cmd.trim().split(",");
        if (args[0].equals(MetadataOperationType.ADD_PATH_TO_MTREE)) {
            String[] leftArgs;
            if (args.length > 4) {
                leftArgs = new String[args.length - 4];
                for (int k = 4; k < args.length; k++) {
                    leftArgs[k - 4] = args[k];
                }
            } else {
                leftArgs = new String[0];
            }
            addPathToMTree(args[1], args[2], args[3], leftArgs);
        } else if (args[0].equals(MetadataOperationType.DELETE_PATH_FROM_MTREE)) {
            deletePathFromMTree(args[1]);
        } else if (args[0].equals(MetadataOperationType.SET_STORAGE_LEVEL_TO_MTREE)) {
            setStorageLevelToMTree(args[1]);
        } else if (args[0].equals(MetadataOperationType.ADD_A_PTREE)) {
            addAPTree(args[1]);
        } else if (args[0].equals(MetadataOperationType.ADD_A_PATH_TO_PTREE)) {
            addPathToPTree(args[1]);
        } else if (args[0].equals(MetadataOperationType.DELETE_PATH_FROM_PTREE)) {
            deletePathFromPTree(args[1]);
        } else if (args[0].equals(MetadataOperationType.LINK_MNODE_TO_PTREE)) {
            linkMNodeToPTree(args[1], args[2]);
        } else if (args[0].equals(MetadataOperationType.UNLINK_MNODE_FROM_PTREE)) {
            unlinkMNodeFromPTree(args[1], args[2]);
        }
    }

    private void initLogStream() {
        if (logWriter == null) {
            File logFile = new File(logFilePath);
            File metadataDir = new File(metadataDirPath);
            if (!metadataDir.exists()) {
                metadataDir.mkdirs();
            }
            FileWriter fileWriter;
            try {

                fileWriter = new FileWriter(logFile, true);
                logWriter = new BufferedWriter(fileWriter);
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * <p>
     * Add one timeseries to metadata. Must invoke the<code>pathExist</code> and <code>getFileNameByPath</code> method
     * first to check timeseries.
     * </p>
     *
     * @param path
     *            the timeseries seriesPath
     * @param dataType
     *            the datetype {@code DataType} for the timeseries
     * @param encoding
     *            the encoding function {@code Encoding} for the timeseries
     * @param args
     * @throws PathErrorException
     * @throws IOException
     * @throws MetadataArgsErrorException
     */
    public void addPathToMTree(String path, String dataType, String encoding, String[] args)
            throws PathErrorException, IOException, MetadataArgsErrorException {

        lock.writeLock().lock();
        try {
            mGraph.addPathToMTree(path, dataType, encoding, args);
            if (writeToLog) {
                initLogStream();
                logWriter.write(MetadataOperationType.ADD_PATH_TO_MTREE + "," + path + "," + dataType + "," + encoding);
                for (int i = 0; i < args.length; i++) {
                    logWriter.write("," + args[i]);
                }
                logWriter.newLine();
                logWriter.flush();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public String deletePathFromMTree(String path) throws PathErrorException, IOException {
        lock.writeLock().lock();
        try {
            checkAndGetDataTypeCache.clear();
            mNodeCache.clear();
            String dataFileName = mGraph.deletePath(path);
            if (writeToLog) {
                initLogStream();
                logWriter.write(MetadataOperationType.DELETE_PATH_FROM_MTREE + "," + path);
                logWriter.newLine();
                logWriter.flush();
            }
            return dataFileName;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void setStorageLevelToMTree(String path) throws PathErrorException, IOException {

        lock.writeLock().lock();
        try {
            checkAndGetDataTypeCache.clear();
            mNodeCache.clear();
            mGraph.setStorageLevel(path);
            if (writeToLog) {
                initLogStream();
                logWriter.write(MetadataOperationType.SET_STORAGE_LEVEL_TO_MTREE + "," + path);
                logWriter.newLine();
                logWriter.flush();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void addAPTree(String pTreeRootName) throws IOException, MetadataArgsErrorException {

        lock.writeLock().lock();
        try {
            mGraph.addAPTree(pTreeRootName);
            if (writeToLog) {
                initLogStream();
                logWriter.write(MetadataOperationType.ADD_A_PTREE + "," + pTreeRootName);
                logWriter.newLine();
                logWriter.flush();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void addPathToPTree(String path) throws PathErrorException, IOException, MetadataArgsErrorException {

        lock.writeLock().lock();
        try {
            mGraph.addPathToPTree(path);
            if (writeToLog) {
                initLogStream();
                logWriter.write(MetadataOperationType.ADD_A_PATH_TO_PTREE + "," + path);
                logWriter.newLine();
                logWriter.flush();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void deletePathFromPTree(String path) throws PathErrorException, IOException {

        lock.writeLock().lock();
        try {
            mGraph.deletePath(path);
            if (writeToLog) {
                initLogStream();
                logWriter.write(MetadataOperationType.DELETE_PATH_FROM_PTREE + "," + path);
                logWriter.newLine();
                logWriter.flush();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void linkMNodeToPTree(String path, String mPath) throws PathErrorException, IOException {

        lock.writeLock().lock();
        try {
            mGraph.linkMNodeToPTree(path, mPath);
            if (writeToLog) {
                initLogStream();
                logWriter.write(MetadataOperationType.LINK_MNODE_TO_PTREE + "," + path + "," + mPath);
                logWriter.newLine();
                logWriter.flush();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void unlinkMNodeFromPTree(String path, String mPath) throws PathErrorException, IOException {

        lock.writeLock().lock();
        try {
            mGraph.unlinkMNodeFromPTree(path, mPath);
            if (writeToLog) {
                initLogStream();
                logWriter.write(MetadataOperationType.UNLINK_MNODE_FROM_PTREE + "," + path + "," + mPath);
                logWriter.newLine();
                logWriter.flush();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Extract the deviceId from given seriesPath
     *
     * @param path
     * @return String represents the deviceId
     */
    public String getDeviceTypeByPath(String path) throws PathErrorException {

        lock.readLock().lock();
        try {
            return mGraph.getDeviceTypeByPath(path);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get series type for given seriesPath
     *
     * @param fullPath
     * @return TSDataType
     * @throws PathErrorException
     */
    public TSDataType getSeriesType(String fullPath) throws PathErrorException {

        lock.readLock().lock();
        try {
            return getSchemaForOnePath(fullPath).dataType;
        } finally {
            lock.readLock().unlock();
        }
    }

    public TSDataType getSeriesType(MNode node, String fullPath) throws PathErrorException {

        lock.readLock().lock();
        try {
            return getSchemaForOnePath(node, fullPath).dataType;
        } finally {
            lock.readLock().unlock();
        }
    }

    public TSDataType getSeriesTypeWithCheck(MNode node, String fullPath) throws PathErrorException {

        lock.readLock().lock();
        try {
            return getSchemaForOnePathWithCheck(node, fullPath).dataType;
        } finally {
            lock.readLock().unlock();
        }
    }

    public TSDataType getSeriesTypeWithCheck(String fullPath) throws PathErrorException {

        lock.readLock().lock();
        try {
            return getSchemaForOnePathWithCheck(fullPath).dataType;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get all device type in current Metadata Tree
     *
     * @return a HashMap contains all distinct device type separated by device Type
     * @throws PathErrorException
     */
    public Map<String, List<ColumnSchema>> getSchemaForAllType() throws PathErrorException {

        lock.readLock().lock();
        try {
            return mGraph.getSchemaForAllType();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get the full Metadata info.
     *
     * @return A {@code Metadata} instance which stores all metadata info
     * @throws PathErrorException
     */
    public Metadata getMetadata() throws PathErrorException {

        lock.readLock().lock();
        try {
            return mGraph.getMetadata();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get the full storage group info.
     *
     * @return A HashSet instance which stores all storage group info
     * @throws PathErrorException
     */
    public HashSet<String> getAllStorageGroup() throws PathErrorException {

        lock.readLock().lock();
        try {
            return mGraph.getAllStorageGroup();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get all ColumnSchemas for given delta object type
     *
     * @param path
     *            A seriesPath represented one Delta object
     * @return a list contains all column schema
     * @throws PathErrorException
     */
    @Deprecated
    public ArrayList<ColumnSchema> getSchemaForOneType(String path) throws PathErrorException {

        lock.readLock().lock();
        try {
            return mGraph.getSchemaForOneType(path);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * <p>
     * Get all ColumnSchemas for the filenode seriesPath
     * </p>
     *
     * @param path
     * @return ArrayList<ColumnSchema> The list of the schema
     */
    public ArrayList<ColumnSchema> getSchemaForFileName(String path) {

        lock.readLock().lock();
        try {
            return mGraph.getSchemaForOneFileNode(path);
        } finally {
            lock.readLock().unlock();
        }
    }

    public Map<String, ColumnSchema> getSchemaMapForOneFileNode(String path) {

        lock.readLock().lock();
        try {
            return mGraph.getSchemaMapForOneFileNode(path);
        } finally {
            lock.readLock().unlock();
        }
    }

    public Map<String, Integer> getNumSchemaMapForOneFileNode(String path) {

        lock.readLock().lock();
        try {
            return mGraph.getNumSchemaMapForOneFileNode(path);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Calculate the count of storage-level nodes included in given seriesPath
     *
     * @param path
     * @return The total count of storage-level nodes.
     * @throws PathErrorException
     */
    public int getFileCountForOneType(String path) throws PathErrorException {

        lock.readLock().lock();
        try {
            return mGraph.getFileCountForOneType(path);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get the file name for given seriesPath Notice: This method could be called if and only if the seriesPath includes
     * one node whose {@code isStorageLevel} is true
     *
     * @param path
     * @return A String represented the file name
     * @throws PathErrorException
     */
    public String getFileNameByPath(String path) throws PathErrorException {

        lock.readLock().lock();
        try {
            return mGraph.getFileNameByPath(path);
        } catch (PathErrorException e) {
            throw new PathErrorException(String.format(e.getMessage()));
        } finally {
            lock.readLock().unlock();
        }
    }

    public String getFileNameByPath(MNode node, String path) throws PathErrorException {

        lock.readLock().lock();
        try {
            return mGraph.getFileNameByPath(node, path);
        } catch (PathErrorException e) {
            throw new PathErrorException(String.format(e.getMessage()));
        } finally {
            lock.readLock().unlock();
        }
    }

    public String getFileNameByPathWithCheck(MNode node, String path) throws PathErrorException {

        lock.readLock().lock();
        try {
            return mGraph.getFileNameByPath(node, path);
        } catch (PathErrorException e) {
            throw new PathErrorException(String.format(e.getMessage()));
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean checkFileNameByPath(String path) {

        lock.readLock().lock();
        try {
            return mGraph.checkFileNameByPath(path);
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<String> getAllFileNames() throws PathErrorException {

        lock.readLock().lock();
        try {
            HashMap<String, ArrayList<String>> res = getAllPathGroupByFileName(ROOT_NAME);
            List<String> fileNameList = new ArrayList<String>();
            for (String fileName : res.keySet()) {
                fileNameList.add(fileName);
            }
            return fileNameList;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * return a HashMap contains all the paths separated by File Name
     */
    public HashMap<String, ArrayList<String>> getAllPathGroupByFileName(String path) throws PathErrorException {
        lock.readLock().lock();
        try {
            return mGraph.getAllPathGroupByFilename(path);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Return all paths for given seriesPath if the seriesPath is abstract. Or return the seriesPath itself.
     */
    public ArrayList<String> getPaths(String path) throws PathErrorException {

        lock.readLock().lock();
        try {
            ArrayList<String> res = new ArrayList<>();
            HashMap<String, ArrayList<String>> pathsGroupByFilename = getAllPathGroupByFileName(path);
            for (ArrayList<String> ps : pathsGroupByFilename.values()) {
                res.addAll(ps);
            }
            return res;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     *
     * @param path
     * @return metadata info of all timeseries under the given seriesPath
     * @throws PathErrorException
     */
    public List<List<String>> getShowTimeseriesPath(String path) throws PathErrorException {
        lock.readLock().lock();
        try {
            return mGraph.getShowTimeseriesPath(path);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * @param path
     * @return All leaf nodes' seriesPath(s) of given seriesPath.
     */
    public List<String> getLeafNodePathInNextLevel(String path) throws PathErrorException {
        lock.readLock().lock();
        try {
            return mGraph.getLeafNodePathInNextLevel(path);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Check whether the seriesPath given exists
     */
    public boolean pathExist(String path) {

        lock.readLock().lock();
        try {
            return mGraph.pathExist(path);
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean pathExist(MNode node, String path) {

        lock.readLock().lock();
        try {
            return mGraph.pathExist(node, path);
        } finally {
            lock.readLock().unlock();
        }
    }

    public MNode getNodeByPath(String path) throws PathErrorException {
        lock.readLock().lock();
        try {
            return mGraph.getNodeByPath(path);
        } finally {
            lock.readLock().unlock();
        }
    }

    public MNode getNodeByDeviceIdFromCache(String deviceId) throws PathErrorException {
        lock.readLock().lock();
        try {
            return mNodeCache.get(deviceId);
        } catch (CacheException e) {
            throw new PathErrorException(e);
        } finally {
            lock.readLock().unlock();
        }
    }

    public MNode getNodeByPathWithCheck(String path) throws PathErrorException {
        lock.readLock().lock();
        try {
            return mGraph.getNodeByPathWithCheck(path);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get ColumnSchema for given seriesPath. Notice: Path must be a complete Path from root to leaf node.
     */
    public ColumnSchema getSchemaForOnePath(String path) throws PathErrorException {

        lock.readLock().lock();
        try {
            return mGraph.getSchemaForOnePath(path);
        } finally {
            lock.readLock().unlock();
        }
    }

    public ColumnSchema getSchemaForOnePath(MNode node, String path) throws PathErrorException {

        lock.readLock().lock();
        try {
            return mGraph.getSchemaForOnePath(node, path);
        } finally {
            lock.readLock().unlock();
        }
    }

    public ColumnSchema getSchemaForOnePathWithCheck(MNode node, String path) throws PathErrorException {

        lock.readLock().lock();
        try {
            return mGraph.getSchemaForOnePathWithCheck(node, path);
        } finally {
            lock.readLock().unlock();
        }
    }

    public ColumnSchema getSchemaForOnePathWithCheck(String path) throws PathErrorException {

        lock.readLock().lock();
        try {
            return mGraph.getSchemaForOnePathWithCheck(path);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Check whether given seriesPath contains a MNode whose {@code MNode.isStorageLevel} is true
     */
    public boolean checkFileLevel(List<Path> path) throws PathErrorException {

        lock.readLock().lock();
        try {
            for (Path p : path) {
                getFileNameByPath(p.getFullPath());
            }
            return true;
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean checkFileLevel(MNode node, List<Path> path) throws PathErrorException {

        lock.readLock().lock();
        try {
            for (Path p : path) {
                getFileNameByPath(node, p.getFullPath());
            }
            return true;
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean checkFileLevel(String path) throws PathErrorException {

        lock.readLock().lock();
        try {
            getFileNameByPath(path);
            return true;
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean checkFileLevelWithCheck(MNode node, String path) throws PathErrorException {

        lock.readLock().lock();
        try {
            getFileNameByPathWithCheck(node, path);
            return true;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void flushObjectToFile() throws IOException {

        lock.writeLock().lock();
        try {
            File dataFile = new File(datafilePath);
            // delete old metadata data file
            if (dataFile.exists()) {
                dataFile.delete();
            }
            File metadataDir = new File(metadataDirPath);
            if (!metadataDir.exists()) {
                metadataDir.mkdirs();
            }
            File tempFile = new File(datafilePath + MetadataConstant.METADATA_TEMP);
            FileOutputStream fos = new FileOutputStream(tempFile);
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(mGraph);
            oos.close();
            // close the logFile stream
            if (logWriter != null) {
                logWriter.close();
                logWriter = null;
            }
            // rename temp file to data file
            tempFile.renameTo(dataFile);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public String getMetadataInString() {

        lock.readLock().lock();
        try {
            return mGraph.toString();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Check whether {@code seriesPath} exists and whether {@code seriesPath} has been set storage level.
     *
     * @param path
     * @return {@link PathCheckRet}
     */
    public PathCheckRet checkPathStorageLevelAndGetDataType(String path) throws PathErrorException {
        try {
            return checkAndGetDataTypeCache.get(path);
        } catch (CacheException e) {
            throw new PathErrorException(e);
        }
    }

    private PathCheckRet loadPathToCache(String path) throws CacheException {
        try {
            if (!pathExist(path)) {
                return new PathCheckRet(false, null);
            }
            List<Path> p = new ArrayList<>();
            p.add(new Path(path));
            if (!checkFileLevel(p)) {
                return new PathCheckRet(false, null);
            }
            return new PathCheckRet(true, getSeriesType(path));
        } catch (PathErrorException e) {
            throw new CacheException(e);
        }
    }

    public static class PathCheckRet {
        private boolean successfully;
        private TSDataType dataType;

        public PathCheckRet(boolean successfully, TSDataType dataType) {
            this.successfully = successfully;
            this.dataType = dataType;
        }

        public boolean isSuccessfully() {
            return successfully;
        }

        public TSDataType getDataType() {
            return dataType;
        }
    }
}
