package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.RootOperator;

/**
 * @Author: LiuDaWei
 * @Create: 2019年11月01日
 */
public class ShowVersionOperator extends RootOperator {
    public ShowVersionOperator() {
        super(SQLConstant.TOK_SHOW);
        this.operatorType = OperatorType.VERSION;
    }
}
