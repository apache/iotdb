package org.apache.iotdb.db.metadata.template;

import org.apache.iotdb.db.mpp.plan.statement.metadata.template.CreateSchemaTemplateStatement;

import java.util.List;

/**
 * @author chenhuangyun
 * @date 2022/6/28
 */
public interface ITemplateManager {

  void createSchemaTemplate(CreateSchemaTemplateStatement statement);

  List<Template> getAllTemplates();

  Template getTemplate(String name);
}
