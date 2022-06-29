package org.apache.iotdb.db.metadata.template;

import java.util.List;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.CreateTemplateStatement;

/**
 * @author chenhuangyun
 * @date 2022/6/28
 **/
public interface ITemplateManager {

    void createSchemaTemplate(CreateTemplateStatement statement);

    List<Template> getAllTemplates();

    Template getTemplate(String name);
}
