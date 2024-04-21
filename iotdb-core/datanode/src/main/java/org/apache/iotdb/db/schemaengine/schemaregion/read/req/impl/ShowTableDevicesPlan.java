package org.apache.iotdb.db.schemaengine.schemaregion.read.req.impl;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.impl.AndFilter;

import java.util.List;

public class ShowTableDevicesPlan {

  private PartialPath devicePattern;

  private SchemaFilter attributeFilter;

  public ShowTableDevicesPlan(PartialPath devicePattern, List<SchemaFilter> attributeFilterList) {
    this.devicePattern = devicePattern;
    this.attributeFilter = getAttributeFilter(attributeFilterList);
  }

  private SchemaFilter getAttributeFilter(List<SchemaFilter> filterList) {
    if (filterList.isEmpty()) {
      return null;
    }
    AndFilter andFilter;
    SchemaFilter latestFilter = filterList.get(0);
    for (int i = 1; i < filterList.size(); i++) {
      andFilter = new AndFilter(latestFilter, filterList.get(i));
      latestFilter = andFilter;
    }
    return latestFilter;
  }

  public PartialPath getDevicePattern() {
    return devicePattern;
  }

  public SchemaFilter getAttributeFilter() {
    return attributeFilter;
  }
}
