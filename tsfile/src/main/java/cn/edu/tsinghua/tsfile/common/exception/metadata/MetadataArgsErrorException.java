package cn.edu.tsinghua.tsfile.common.exception.metadata;

/**
 * If query metadata constructs schema but passes illegal parameters to
 * EncodingConvertor or DataTypeConvertor,this exception will be threw.
 *
 * @author kangrong
 */
public class MetadataArgsErrorException extends Exception {

    private static final long serialVersionUID = 3415275599091623570L;

    public MetadataArgsErrorException(String msg) {
        super(msg);
    }

}
