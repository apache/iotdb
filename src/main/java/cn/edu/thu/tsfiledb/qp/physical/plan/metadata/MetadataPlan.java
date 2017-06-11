package cn.edu.thu.tsfiledb.qp.physical.plan.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfiledb.exception.ArgsErrorException;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.metadata.MManager;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.executor.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.logical.operator.Operator.OperatorType;
import cn.edu.thu.tsfiledb.qp.logical.operator.root.metadata.MetadataOperator.NamespaceType;
import cn.edu.thu.tsfiledb.qp.physical.plan.DeletePlan;
import cn.edu.thu.tsfiledb.qp.physical.plan.PhysicalPlan;

/**
 *
 * @author kangrong
 *
 */
public class MetadataPlan extends PhysicalPlan {
	private static final Logger logger = LoggerFactory.getLogger(MetadataPlan.class);
	private final NamespaceType namespaceType;
	private Path path;
	private String dataType;
	private String encoding;
	private String[] encodingArgs;

	public Path getPath() {
		return path;
	}

	public void setPath(Path path) {
		this.path = path;
	}

	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	public String getEncoding() {
		return encoding;
	}

	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	public String[] getEncodingArgs() {
		return encodingArgs;
	}

	public void setEncodingArgs(String[] encodingArgs) {
		this.encodingArgs = encodingArgs;
	}

	public NamespaceType getNamespaceType() {
		return namespaceType;
	}

	public MetadataPlan(NamespaceType namespaceType, Path path, String dataType, String encoding,
			String[] encodingArgs) {
		super(false, OperatorType.METADATA);
		this.namespaceType = namespaceType;
		this.path = path;
		this.dataType = dataType;
		this.encoding = encoding;
		this.encodingArgs = encodingArgs;
	}

	public boolean processNonQuery(QueryProcessExecutor executor) throws ProcessorException {
		MManager mManager = executor.getMManager();
		try {
			switch (namespaceType) {
			case ADD_PATH:
				mManager.addAPathToMTree(path.getFullPath(), dataType, encoding, encodingArgs);
				break;
			case DELETE_PATH:
				try {
					// First delete all data interactive
					deleteAllData(executor);
					// Then delete the metadata
					mManager.deletePathFromMTree(path.getFullPath());
				} catch (Exception e) {
					return true;
				}
				break;
			case SET_FILE_LEVEL:
				mManager.setStorageLevelToMTree(path.getFullPath());
				break;
			default:
				throw new ProcessorException("unknown namespace type:" + namespaceType);
			}
		} catch (PathErrorException | IOException | ArgsErrorException e) {
			throw new ProcessorException("meet error in " + namespaceType + " . " + e.getMessage());
		}
		return true;
	}

	private void deleteAllData(QueryProcessExecutor config) throws PathErrorException, ProcessorException {
		MManager mManager = config.getMManager();
		ArrayList<String> pathList = mManager.getPaths(path.getFullPath());
		for (String p : pathList) {
			if(mManager.pathExist(p)){
				DeletePlan deletePlan = new DeletePlan();
				deletePlan.setPath(new Path(p));
				deletePlan.setDeleteTime(Long.MAX_VALUE);
				deletePlan.processNonQuery(config);
			}
		}
	}

	@Override
	public List<Path> getPaths() {
		List<Path> ret = new ArrayList<>();
		if (path != null)
			ret.add(path);
		return ret;
	}
}
