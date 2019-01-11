package cn.edu.tsinghua.iotdb.qp.logical.sys;

import java.util.List;

import cn.edu.tsinghua.iotdb.qp.logical.RootOperator;
import cn.edu.tsinghua.tsfile.read.common.Path;

/**
 * this class maintains information in Metadata.namespace statement
 */
public class MetadataOperator extends RootOperator {

    public MetadataOperator(int tokenIntType, NamespaceType type) {
        super(tokenIntType);
        namespaceType = type;
        switch (type) {
            case SET_FILE_LEVEL:
            case ADD_PATH:
                operatorType = OperatorType.SET_STORAGE_GROUP;
                break;
            case DELETE_PATH:
                operatorType = OperatorType.DELETE_TIMESERIES;
                break;
        }
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
