package cn.edu.thu.tsfiledb.qp.logical.operator.root.sfw;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * this class extends {@code RootOperator} and process delete statement
 * 
 * @author kangrong
 *
 */
public class DeleteOperator extends SFWOperator {

    private static Logger LOG = LoggerFactory.getLogger(DeleteOperator.class);

    public DeleteOperator(int tokenIntType) {
        super(tokenIntType);
        operatorType = OperatorType.DELETE;
    }

}
