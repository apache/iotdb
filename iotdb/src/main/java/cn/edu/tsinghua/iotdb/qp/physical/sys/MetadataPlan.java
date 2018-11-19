package cn.edu.tsinghua.iotdb.qp.physical.sys;

import java.util.ArrayList;
import java.util.List;

import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.iotdb.qp.logical.Operator;
import cn.edu.tsinghua.iotdb.qp.logical.sys.MetadataOperator;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

/**
 *
 * @author kangrong
 * @author qiaojialin
 */
public class MetadataPlan extends PhysicalPlan {
	private final MetadataOperator.NamespaceType namespaceType;
	private Path path;
	private String dataType;
	private String encoding;
	private String[] encodingArgs;
	
	private List<Path> deletePathList;

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

	public MetadataOperator.NamespaceType getNamespaceType() {
		return namespaceType;
	}

	public MetadataPlan(MetadataOperator.NamespaceType namespaceType, Path path, String dataType, String encoding,
                        String[] encodingArgs, List<Path> deletePathList) {
		super(false, Operator.OperatorType.METADATA);
		this.namespaceType = namespaceType;
		this.path = path;
		this.dataType = dataType;
		this.encoding = encoding;
		this.encodingArgs = encodingArgs;
		this.deletePathList = deletePathList;
		switch (namespaceType){
			case SET_FILE_LEVEL:
			case ADD_PATH:
				setOperatorType(Operator.OperatorType.SET_STORAGE_GROUP);
				break;
			case DELETE_PATH:
				setOperatorType(Operator.OperatorType.DELETE_TIMESERIES);
				break;
		}
	}

	@Override
	public String toString() {
		String ret =  "path: " + path +
				"\ndataType: " + dataType +
				"\nencoding: " + encoding +
				"\nnamespace type: " + namespaceType +
				"\nargs: ";
		for (String arg: encodingArgs) {
			ret = ret + arg + ",";
		}
		return ret;
	}
	
	public void addDeletePath(Path path){
		deletePathList.add(path);
	}
	
	public List<Path> getDeletePathList() {
		return deletePathList;
	}

	@Override
	public List<Path> getPaths() {
		if(deletePathList != null){
			return deletePathList;
		}
		
		List<Path> ret = new ArrayList<>();
		if (path != null)
			ret.add(path);
		return ret;
	}
}
