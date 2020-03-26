package org.apache.iotdb.db.engine.merge.sizeMerge.regularization;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.engine.merge.IMergeFileSelector;
import org.apache.iotdb.db.engine.merge.MergeTest;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.sizeMerge.regularization.selector.RegularizationMaxFileSelector;
import org.apache.iotdb.db.engine.merge.utils.SelectorContext;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.tsfile.utils.Pair;
import org.junit.Test;

public class MaxFileMergeFileSelectorTest extends MergeTest {

  @Test
  public void testFullSelection() throws MergeException, IOException {
    IMergeFileSelector mergeFileSelector = new RegularizationMaxFileSelector(seqResources,
        Long.MAX_VALUE);
    Pair<MergeResource, SelectorContext> selectRes = mergeFileSelector.selectMergedFiles();
    MergeResource mergeResource = selectRes.left;
    List<TsFileResource> seqSelected = mergeResource.getSeqFiles();
    List<TsFileResource> unseqSelected = mergeResource.getUnseqFiles();
    assertEquals(seqResources, seqSelected);
    assertEquals(new ArrayList<>(), unseqSelected);
    mergeResource.clear();
  }

  @Test
  public void testNonSelection() throws MergeException, IOException {
    IMergeFileSelector mergeFileSelector = new RegularizationMaxFileSelector(seqResources, 1);
    Pair<MergeResource, SelectorContext> selectRes = mergeFileSelector.selectMergedFiles();
    MergeResource mergeResource = selectRes.left;
    assertEquals(0, mergeResource.getUnseqFiles().size());
    assertEquals(0, mergeResource.getSeqFiles().size());
    mergeResource.clear();
  }
}
