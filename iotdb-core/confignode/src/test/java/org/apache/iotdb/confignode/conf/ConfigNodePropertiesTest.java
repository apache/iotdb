package org.apache.iotdb.confignode.conf;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.Test;

import java.util.Properties;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

public class ConfigNodePropertiesTest {
  @Test
  public void TrimPropertiesOnly() {
    JavaClasses allClasses =
        new ClassFileImporter()
            .withImportOption(new ImportOption.DoNotIncludeTests())
            .importPackages("org.apache.iotdb");

    ArchRule rule =
        noClasses()
            .that()
            .areAssignableTo("org.apache.iotdb.confignode.conf.ConfigNodeDescriptor")
            .should()
            .callMethod(Properties.class, "getProperty", String.class)
            .orShould()
            .callMethod(Properties.class, "getProperty", String.class, String.class);

    rule.check(allClasses);
  }
}
