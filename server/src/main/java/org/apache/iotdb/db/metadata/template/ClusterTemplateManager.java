package org.apache.iotdb.db.metadata.template;

import org.apache.iotdb.db.mpp.plan.statement.metadata.template.CreateSchemaTemplateStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chenhuangyun
 * @date 2022/6/28
 */
public class ClusterTemplateManager implements ITemplateManager {

  @Override
  public void createSchemaTemplate(CreateSchemaTemplateStatement statement) {
    statement.getAlignedDeviceId();
  }

  @Override
  public List<Template> getAllTemplates() {
    return new ArrayList<Template>();
  }

  @Override
  public Template getTemplate(String name) {
    return new Template();
  }
}
