package org.apache.iotdb.db.metadata.template;

import java.util.List;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.CreateTemplateStatement;

/**
 * @author chenhuangyun
 * @date 2022/6/28
 **/
public class ClusterTemplateManager implements  ITemplateManager{

    @Override
    public void createSchemaTemplate(CreateTemplateStatement statement) {

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
