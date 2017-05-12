package cn.edu.thu.tsfiledb.metadata;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.edu.thu.tsfile.common.conf.TSFileDescriptor;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.conf.TSFileDBDescriptor;
import cn.edu.thu.tsfiledb.exception.MetadataArgsErrorException;
import cn.edu.thu.tsfiledb.exception.PathErrorException;

/**
 * This class takes the responsibility of serialization of all the metadata info
 * and persistent it into files. This class contains all the interfaces to
 * modify the metadata for delta system. All the operations will be write into
 * the logs temporary in case the downtime of the delta system.
 * 
 * @author Jinrui Zhang
 *
 */
public class MManager {
	private static MManager manager = new MManager();

	// The file storing the serialize info for metadata
	private String datafilePath;
	// log file path
	private String logFilePath;

	private MGraph mGraph;
	private BufferedWriter bw;
	private boolean writeToLog;

	private MManager() {
		writeToLog = false;
		
		String folderPath = TSFileDBDescriptor.getInstance().getConfig().metadataDir;
		datafilePath = folderPath + "/mdata.obj";
		logFilePath = folderPath + "/mlog.txt";
		init();
	}

	private void init() {
		try {
			File file = new File(datafilePath);
			// inital MGraph from file
			if (file.exists()) {
				FileInputStream fis = new FileInputStream(file);
				ObjectInputStream ois = new ObjectInputStream(fis);
				mGraph = (MGraph) ois.readObject();
				ois.close();
				fis.close();

			} else {
				mGraph = new MGraph("root");
			}

			// recover operation from log file
			File logFile = new File(logFilePath);
			if (logFile.exists()) {
				FileReader fr;
				fr = new FileReader(logFile);
				BufferedReader br = new BufferedReader(fr);
				String cmd;
				while ((cmd = br.readLine()) != null) {
					operation(cmd);
				}
				br.close();
			} else {
				if (!logFile.getParentFile().exists()) {
					logFile.getParentFile().mkdirs();
				}
				logFile.createNewFile();
			}

			FileWriter fw = new FileWriter(logFile, true);
			bw = new BufferedWriter(fw);
			writeToLog = true;

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (PathErrorException e) {
			e.printStackTrace();
		} catch (MetadataArgsErrorException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Clear all metadata info
	 */
	public void clear() {
		this.mGraph = new MGraph("root");
	}

	public void deleteLogAndDataFiles() {
		File file = new File(logFilePath);
		if (file.exists()) {
			file.delete();
		}
		File dataFile = new File(datafilePath);
		if (dataFile.exists()) {
			dataFile.delete();
		}
	}

	private void operation(String cmd) throws PathErrorException, IOException, MetadataArgsErrorException {
		String args[] = cmd.trim().split(",");
		if (args[0].equals("0")) {
			String[] leftArgs;
			if (args.length > 4) {
				leftArgs = new String[args.length - 4];
				for (int k = 4; k < args.length; k++) {
					leftArgs[k - 4] = args[k];
				}
			} else {
				leftArgs = new String[0];
			}
			addAPathToMTree(args[1], args[2], args[3], leftArgs);
		} else if (args[0].equals(MetadataOperationType.DELETE_PATH_FROM_MTREE)) {
			deletePathFromMTree(args[1]);
		} else if (args[0].equals(MetadataOperationType.SET_STORAGE_LEVEL_TO_MTREE)) {
			setStorageLevelToMTree(args[1]);
		} else if (args[0].equals(MetadataOperationType.ADD_A_PTREE)) {
			addAPTree(args[1]);
		} else if (args[0].equals(MetadataOperationType.ADD_A_PATH_TO_PTREE)) {
			addAPathToPTree(args[1]);
		} else if (args[0].equals(MetadataOperationType.DELETE_PATH_FROM_PTREE)) {
			deletePathFromPTree(args[1]);
		} else if (args[0].equals(MetadataOperationType.LINK_MNODE_TO_PTREE)) {
			linkMNodeToPTree(args[1], args[2]);
		} else if (args[0].equals(MetadataOperationType.UNLINK_MNODE_FROM_PTREE)) {
			unlinkMNodeFromPTree(args[1], args[2]);
		}
	}

	/**
	 * operation: Add a path to Metadata Tree
	 */
	public int addAPathToMTree(String path, String dataType, String encoding, String[] args)
			throws PathErrorException, IOException, MetadataArgsErrorException {
		int addCount = mGraph.addPathToMTree(path, dataType, encoding, args);
		if (writeToLog) {
			bw.write(MetadataOperationType.ADD_PATH_TO_MTREE + "," + path + "," + dataType + "," + encoding);
			for (int i = 0; i < args.length; i++) {
				bw.write("," + args[i]);
			}
			bw.newLine();
			bw.flush();
		}
		return addCount;
	}

	public void deletePathFromMTree(String path) throws PathErrorException, IOException {
		mGraph.deletePath(path);
		if (writeToLog) {
			bw.write(MetadataOperationType.DELETE_PATH_FROM_MTREE + "," + path);
			bw.newLine();
			bw.flush();
		}
	}

	public void setStorageLevelToMTree(String path) throws PathErrorException, IOException {
		mGraph.setStorageLevel(path);
		if (writeToLog) {
			bw.write(MetadataOperationType.SET_STORAGE_LEVEL_TO_MTREE + "," + path);
			bw.newLine();
			bw.flush();
		}
	}


	public void addAPTree(String pTreeRootName) throws IOException, MetadataArgsErrorException {
		mGraph.addAPTree(pTreeRootName);
		if (writeToLog) {
			bw.write(MetadataOperationType.ADD_A_PTREE + "," + pTreeRootName);
			bw.newLine();
			bw.flush();
		}
	}

	public void addAPathToPTree(String path) throws PathErrorException, IOException, MetadataArgsErrorException {
		mGraph.addPathToPTree(path);
		if (writeToLog) {
			bw.write(MetadataOperationType.ADD_A_PATH_TO_PTREE + "," + path);
			bw.newLine();
			bw.flush();
		}
	}


	public void deletePathFromPTree(String path) throws PathErrorException, IOException {
		mGraph.deletePath(path);
		if (writeToLog) {
			bw.write(MetadataOperationType.DELETE_PATH_FROM_PTREE + "," + path);
			bw.newLine();
			bw.flush();
		}
	}

	public void linkMNodeToPTree(String path, String mPath) throws PathErrorException, IOException {
		mGraph.linkMNodeToPTree(path, mPath);
		if (writeToLog) {
			bw.write(MetadataOperationType.LINK_MNODE_TO_PTREE + "," + path + "," + mPath);
			bw.newLine();
			bw.flush();
		}
	}

	public void unlinkMNodeFromPTree(String path, String mPath) throws PathErrorException, IOException {
		mGraph.unlinkMNodeFromPTree(path, mPath);
		if (writeToLog) {
			bw.write(MetadataOperationType.UNLINK_MNODE_FROM_PTREE + "," + path + "," + mPath);
			bw.newLine();
			bw.flush();
		}
	}

	/**
	 * Extract the DeltaObjectId from given path
	 * @param path
	 * @return String represents the DeltaObjectId
	 */
	public String getDeltaObjectTypeByPath(String path) throws PathErrorException {
		return mGraph.getDeltaObjectTypeByPath(path);
	}

	/**
	 * Get series type for given path
	 * 
	 * @param fullPath
	 * @return TSDataType
	 * @throws PathErrorException
	 */
	public TSDataType getSeriesType(String fullPath) throws PathErrorException {
		return getSchemaForOnePath(fullPath).dataType;
	}

	/**
	 * Get all DeltaObject type in current Metadata Tree
	 * 
	 * @return a HashMap contains all distinct DeltaObject type separated by
	 *         DeltaObject Type
	 * @throws PathErrorException
	 */
	public Map<String, List<ColumnSchema>> getSchemaForAllType() throws PathErrorException {
		return mGraph.getSchemaForAllType();
	}

	/**
	 * Get the full Metadata info.
	 * 
	 * @return A {@code Metadata} instance which stores all metadata info
	 * @throws PathErrorException
	 */
	public Metadata getMetadata() throws PathErrorException {
		return mGraph.getMetadata();
	}

	/**
	 * Get all ColumnSchemas for given delta object type
	 * 
	 * @param path A path represented one Delta object
	 * @return a list contains all column schema
	 * @throws PathErrorException
	 */
	public ArrayList<ColumnSchema> getSchemaForOneType(String path) throws PathErrorException {
		return mGraph.getSchemaForOneType(path);
	}

	/**
	 * Calculate the count of storage-level nodes included in given path
	 * @param path
	 * @return The total count of storage-level nodes.
	 * @throws PathErrorException
	 */
	public int getFileCountForOneType(String path) throws PathErrorException {
		return mGraph.getFileCountForOneType(path);
	}

	/**
	 * Get the file name for given path Notice: This method could be called if
	 * and only if the path includes one node whose {@code isStorageLevel} is
	 * true
	 * 
	 * @param path
	 * @return A String represented the file name
	 * @throws PathErrorException
	 */
	public String getFileNameByPath(String path) throws PathErrorException {
		return mGraph.getFileNameByPath(path);
	}

	/**
	 * return a HashMap contains all the paths separated by File Name
	 */
	public HashMap<String, ArrayList<String>> getAllPathGroupByFileName(String path) throws PathErrorException {
		return mGraph.getAllPathGroupByFilename(path);
	}

	/**
	 * return all paths for given path if the path is abstract.Or return the
	 * path itself.
	 */
	public ArrayList<String> getPaths(String path) throws PathErrorException {
		ArrayList<String> res = new ArrayList<>();
		HashMap<String, ArrayList<String>> pathsGroupByFilename = getAllPathGroupByFileName(path);
		for (ArrayList<String> ps : pathsGroupByFilename.values()) {
			res.addAll(ps);
		}
		return res;
	}

	/**
	 * Check whether the path given exists
	 */
	public boolean pathExist(String path) {
		return mGraph.pathExist(path);
	}

	/**
	 * Get ColumnSchema for given path. Notice: Path must be a complete Path
	 * from root to leaf node.
	 */
	public ColumnSchema getSchemaForOnePath(String path) throws PathErrorException {
		return mGraph.getSchemaForOnePath(path);
	}

	/**
	 * Check whether given path contains a MNode whose
	 * {@code MNode.isStorageLevel} is true
	 */
	public boolean checkFileLevel(List<Path> path) throws PathErrorException {
		for (Path p : path) {
			getFileNameByPath(p.getFullPath());
		}
		return true;
	}

	/**
	 * Persistent the MGraph instance into file
	 */
	public void flushObjectToFile() throws IOException {
		File newDataFile = new File(datafilePath + ".backup");
		FileOutputStream fos = new FileOutputStream(newDataFile);
		ObjectOutputStream oos = new ObjectOutputStream(fos);
		oos.writeObject(mGraph);
		oos.close();

		// delete log file
		File logFile = new File(logFilePath);
		logFile.delete();

		// Copy New DataFile to OldDataFile
		FileOutputStream oldFileFos = new FileOutputStream(datafilePath);
		FileChannel outChannel = oldFileFos.getChannel();
		FileInputStream newFileFis = new FileInputStream(datafilePath + ".backup");
		FileChannel inChannel = newFileFis.getChannel();
		inChannel.transferTo(0, inChannel.size(), outChannel);
		outChannel.close();
		inChannel.close();
		oldFileFos.close();
		newFileFis.close();

		// delete DataFileBackUp
		newDataFile.delete();
	}

	public String getMetadataInString() {
		return mGraph.toString();
	}

	public static MManager getInstance() {
		return manager;
	}
}
