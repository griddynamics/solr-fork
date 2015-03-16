package org.apache.solr.cloud;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.solr.BaseDistributedSearchTestCase.ShardsFixed;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.BeforeClass;
import org.junit.Test;

@Slow
@SuppressCodecs({"Lucene3x", "Lucene40","Lucene41","Lucene42","Lucene45"})
public class InPlaceUpdateDistribTest extends AbstractFullDistribZkTestBase {
  @BeforeClass
  public static void beforeSuperClass() {
    schemaString = "schema15.xml";      // we need a string id
  }

  public InPlaceUpdateDistribTest() {
    super();
    sliceCount = 1;
  }

  void indexr(SolrClient server, Object ...fields) throws IOException, SolrServerException {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    server.add(doc);
  }

  private void docValuesUpdateTest() throws Exception,
      IOException {
    int numDocs = 500;

    for(int i=0; i<numDocs; i++)
      indexr("id", i, t1, "title"+i);
    commit();

    List<Long> versions = new ArrayList<Long>();

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fl", "id,field(ratings),_version_");
    params.add("rows", ""+numDocs);
    params.add("sort", "_version_ asc");
    SolrDocumentList results = clients.get(0).query(params).getResults();
    for (SolrDocument doc: results)
      versions.add(Long.parseLong(doc.get("_version_").toString()));

    List<Integer> ratingsList = new ArrayList<Integer>();
    for (int i=0; i<numDocs; i++)
      ratingsList.add(r.nextInt());

    // update doc, set
    for (int i=0; i<numDocs; i++) {
      indexr(clients.get(random().nextInt(clients.size())), "id", i, "ratings",
          createMap("set", (""+ratingsList.get(i)) ));
      if (rarely())
        commit(); // to have several segments
    }
    commit();

    results = clients.get(random().nextInt(clients.size())).query(params).getResults();
    int counter = 0;
    for (SolrDocument doc: results) {
      long v = Long.parseLong(doc.get("_version_").toString());
      int r = Integer.parseInt(doc.get("field(ratings)").toString());

      assertEquals(versions.get(counter).longValue(), v);
      assertEquals(ratingsList.get(counter).intValue(), r);
      counter++;
    }

    // update doc, increment
    for (int i=0; i<numDocs; i++) {
      int inc = r.nextInt(1000) * (r.nextBoolean()?-1:1);
      ratingsList.set(i, ratingsList.get(i)+inc);
      indexr(clients.get(random().nextInt(clients.size())), "id", i, "ratings",
          createMap("inc", (""+inc) ));
      if (rarely())
        commit(); // to have several segments
    }
    commit();

    results = clients.get(random().nextInt(clients.size())).query(params).getResults();
    counter = 0;
    for (SolrDocument doc: results) {
      long v = Long.parseLong(doc.get("_version_").toString());
      int r = Integer.parseInt(doc.get("field(ratings)").toString());

      assertEquals(versions.get(counter).longValue(), v);
      assertEquals(ratingsList.get(counter).intValue(), r);
      counter++;
    }

  }

  /**
   * Strings at even index are keys, odd-index strings are values in the
   * returned map
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private Map createMap(Object... args) {
    Map result = new LinkedHashMap();

    if (args == null || args.length == 0)
      return result;

    for (int i = 0; i < args.length - 1; i += 2)
      result.put(args[i], args[i + 1]);

    return result;
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  protected SolrInputDocument addRandFields(SolrInputDocument sdoc) {
    return sdoc;
  }

  /* 
   *
   */
  @Override
  public void doTest() throws Exception {
    docValuesUpdateTest();
  }

}