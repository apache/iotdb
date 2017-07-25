package cn.edu.thu.tsfiledb.qp.physical.sys;

import java.util.ArrayList;
import java.util.List;

import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.logical.Operator.OperatorType;
import cn.edu.thu.tsfiledb.qp.logical.sys.MetadataOperator.NamespaceType;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;

/**
 *
 * @author kangrong
 * @author qiaojialin
 */
public class MetadataPlan extends PhysicalPlan {
	private final NamespaceType namespaceType;
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

	public NamespaceType getNamespaceType() {
		return namespaceType;
	}

	public MetadataPlan(NamespaceType namespaceType, Path path, String dataType, String encoding,
			String[] encodingArgs, List<Path> deletePathList) {
		super(false, OperatorType.METADATA);
		this.namespaceType = namespaceType;
		this.path = path;
		this.dataType = dataType;
		this.encoding = encoding;
		this.encodingArgs = encodingArgs;
		this.deletePathList = deletePathList;
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
