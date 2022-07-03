package org.apache.iotdb.db.mpp.plan.statement.metadata.template;

import org.apache.iotdb.db.mpp.plan.constant.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowStatement;

/**
 * @author chenhuangyun
 * @date 2022/6/30
 **/
public class ShowNodesInSchemaTemplateStatement extends ShowStatement {

    private String templateName;

    public ShowNodesInSchemaTemplateStatement(String templateName) {
        super();
        statementType = StatementType.SHOW_NODES_IN_SCHEMA_TEMPLATE;
        this.templateName = templateName;
    }

    @Override
    public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
        return visitor.visitShowNodesInSchemaTemplate(this, context);
    }

    public String getTemplateName() {
        return templateName;
    }

    public void setTemplateName(String templateName) {
        this.templateName = templateName;
    }
}
