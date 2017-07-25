package cn.edu.thu.tsfiledb.qp.logical.sys;

import java.util.List;

import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.logical.RootOperator;

/**
 * this class maintains information in Metadata.namespace statement
 * 
 * @author kangrong
 *
 */
public class MetadataOperator extends RootOperator {

    public MetadataOperator(int tokenIntType, NamespaceType type) {
        super(tokenIntType);
        namespaceType = type;
        operatorType = OperatorType.METADATA;
    }

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

    public List<Path> getDeletePathList() {
		return deletePathList;
	}

	public void setDeletePathList(List<Path> deletePathList) {
		this.deletePathList = deletePathList;
	}

	public enum NamespaceType {
        ADD_PATH, DELETE_PATH, SET_FILE_LEVEL
    }
}
