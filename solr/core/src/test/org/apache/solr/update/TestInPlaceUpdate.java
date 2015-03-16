/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.update;

import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.Test;


@SuppressCodecs({"Lucene3x", "Lucene40","Lucene41","Lucene42","Lucene45"})
public class TestInPlaceUpdate extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-tlog.xml","schema15.xml");
  }

  @Test
  public void testUpdatingDocValues() throws Exception {
    clearIndex();
    assertU(commit("softCommit","false"));

    long version1 = addAndGetVersion(sdoc("id","1", "title","first"), null);
    long version2 = addAndGetVersion(sdoc("id","2", "title","second"), null);
    long version3 = addAndGetVersion(sdoc("id","3", "title","third"), null);
    assertU(commit("softCommit","false"));

    assertQ(req("q","*:*"), "//*[@numFound='3']");

    // Check docValues were "set"
    addAndGetVersion(sdoc("id","1", "ratings", map("set", 200)), null);
    addAndGetVersion(sdoc("id","2", "ratings", map("set", 300)), null);
    addAndGetVersion(sdoc("id","3", "ratings", map("set", 100)), null);
    assertU(commit("softCommit","false"));

    assertQ(req("q","*:*", "fl","id,field(ratings),_version_", "sort", "id asc"),
        "//*[@numFound='3']",
        "//result/doc[1]/int[@name='field(ratings)'][.='200']",
        "//result/doc[1]/long[@name='_version_'][.='"+version1+"']",
        "//result/doc[2]/int[@name='field(ratings)'][.='300']",
        "//result/doc[2]/long[@name='_version_'][.='"+version2+"']",
        "//result/doc[3]/int[@name='field(ratings)'][.='100']",
        "//result/doc[3]/long[@name='_version_'][.='"+version3+"']");

    // Check docValues are "inc"ed
    addAndGetVersion(sdoc("id","1", "ratings", map("inc", 1)), null);
    addAndGetVersion(sdoc("id","2", "ratings", map("inc", -2)), null);
    addAndGetVersion(sdoc("id","3", "ratings", map("inc", 3)), null);
    assertU(commit("softCommit", "false"));
    assertQ(req("q","*:*", "fl","id,field(ratings),_version_", "sort", "id asc"),
        "//*[@numFound='3']",
        "//result/doc[1]/int[@name='field(ratings)'][.='201']",
        "//result/doc[1]/long[@name='_version_'][.='"+version1+"']",
        "//result/doc[2]/int[@name='field(ratings)'][.='298']",
        "//result/doc[2]/long[@name='_version_'][.='"+version2+"']",
        "//result/doc[3]/int[@name='field(ratings)'][.='103']",
        "//result/doc[3]/long[@name='_version_'][.='"+version3+"']");

    // Check back to back "inc"s are working (off the transaction log)
    addAndGetVersion(sdoc("id","1", "ratings", map("inc", 1)), null); // new value should be 202
    addAndGetVersion(sdoc("id","1", "ratings", map("inc", 2)), null); // new value should be 204
    assertU(commit("softCommit", "false"));
    assertQ(req("q","id:1", "fl","id,field(ratings),_version_"),
        "//result/doc[1]/int[@name='field(ratings)'][.='204']",
        "//result/doc[1]/long[@name='_version_'][.='"+version1+"']");

    // Now let the document be atomically updated (non-inplace), ensure the old docvalue is part of new doc
    long newVersion1 = addAndGetVersion(sdoc("id","1", "title", map("set","new first")/*, "ratings", map("inc", 6)*/), null);
    assertU(commit("softCommit", "false"));
    assertQ(req("q","id:1", "fl","id,title,field(ratings),_version_"),
        "//result/doc[1]/int[@name='field(ratings)'][.='204']",
        "//result/doc[1]/str[@name='title'][.='new first']",
        "//result/doc[1]/long[@name='_version_'][.='"+newVersion1+"']");
    assertTrue(newVersion1>version1);

    // Check if atomic update with "inc" to a docValue works
    long newVersion2 = addAndGetVersion(sdoc("id","2", "title", map("set","new second"), "ratings", map("inc", 2)), null);
    assertU(commit("softCommit", "false"));
    assertQ(req("q","id:2", "fl","id,title,field(ratings),_version_"),
        "//result/doc[1]/int[@name='field(ratings)'][.='300']",
        "//result/doc[1]/str[@name='title'][.='new second']",
        "//result/doc[1]/long[@name='_version_'][.='"+newVersion2+"']");
    assertTrue(newVersion2>version2);

    // Check if docvalue "inc" update works for a newly created document, which is not yet committed
    // Case1: docvalue was supplied during add of new document
    long version4 = addAndGetVersion(sdoc("id","4", "title","fourth", "ratings","400"), null);
    addAndGetVersion(sdoc("id","4", "ratings", map("inc", 1)), null);
    assertU(commit("softCommit", "false"));
    assertQ(req("q","id:4", "fl","id,field(ratings),_version_"),
        "//result/doc[1]/int[@name='field(ratings)'][.='401']",
        "//result/doc[1]/long[@name='_version_'][.='"+version4+"']");

    // Check if docvalue "inc" update works for a newly created document, which is not yet committed
    // Case2: docvalue was not supplied during add of new document, should assume default
    long version5 = addAndGetVersion(sdoc("id","5", "title","fifth"), null);
    addAndGetVersion(sdoc("id","5", "ratings", map("inc", 1)), null);
    assertU(commit("softCommit", "false"));
    assertQ(req("q","id:5", "fl","id,field(ratings),_version_"),
        "//result/doc[1]/int[@name='field(ratings)'][.='1']",
        "//result/doc[1]/long[@name='_version_'][.='"+version5+"']");

    // Check if docvalue "set" update works for a newly created document, which is not yet committed
    long version6 = addAndGetVersion(sdoc("id","6", "title","sixth"), null);
    addAndGetVersion(sdoc("id","6", "ratings", map("set", 600)), null);
    assertU(commit("softCommit", "false"));
    assertQ(req("q","id:6", "fl","id,field(ratings),_version_"),
        "//result/doc[1]/int[@name='field(ratings)'][.='600']",
        "//result/doc[1]/long[@name='_version_'][.='"+version6+"']");

  }


}