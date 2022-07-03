package org.apache.iotdb.db.metadata.template;

import java.util.List;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.CreateSchemaTemplateStatement;

/**
 * @author chenhuangyun
 * @date 2022/6/28
 **/
public class ClusterTemplateManager implements  ITemplateManager{

    @Override
    public void createSchemaTemplate(CreateSchemaTemplateStatement statement) {

    }

    @Override
    public List<Template> getAllTemplates() {
        return null;
    }

    @Override
    public Template getTemplate(String name) {
        return null;
    }
}
