/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.utils;

import org.apache.iotdb.commons.utils.IOUtils;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.SchemaFile;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaMethodCall;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.EvaluationResult;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

public class FileChannelWriteArchitectureTest {

  private static final ArchRule RULE =
      noClasses()
          .should()
          .callMethodWhere(
              new DescribedPredicate<JavaMethodCall>("call FileChannel.write directly") {
                @Override
                public boolean test(JavaMethodCall call) {
                  return call.getTarget().getName().equals("write")
                      && call.getTarget().getOwner().isAssignableTo(FileChannel.class);
                }
              })
          .because("schema-file writes must handle short writes and fail on zero progress");

  @Test
  public void schemaFileProductionCodeMustWriteFully() {
    RULE.check(
        new ClassFileImporter()
            .withImportOption(new ImportOption.DoNotIncludeTests())
            .importPackages(SchemaFile.class.getPackage().getName()));
  }

  @Test
  public void ruleRejectsEveryRawWriteOverload() {
    EvaluationResult result = RULE.evaluate(new ClassFileImporter().importClasses(RawWrites.class));

    Assert.assertTrue(result.hasViolation());
    Assert.assertEquals(4, result.getFailureReport().getDetails().size());
  }

  @Test
  public void ruleAllowsWriteFully() {
    RULE.check(new ClassFileImporter().importClasses(CompleteWrites.class));
  }

  private static class RawWrites {
    void write(FileChannel channel, ByteBuffer buffer, ByteBuffer[] buffers) throws IOException {
      channel.write(buffer);
      channel.write(buffer, 0);
      channel.write(buffers);
      channel.write(buffers, 0, buffers.length);
    }
  }

  private static class CompleteWrites {
    void write(FileChannel channel, ByteBuffer buffer) throws IOException {
      IOUtils.writeFully(channel, buffer);
      IOUtils.writeFully(channel, buffer, 0);
    }
  }
}
