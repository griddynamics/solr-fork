package org.apache.solr.search.join;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;

public abstract class AbstractBJQTestCase extends SolrTestCaseJ4 {
  public static class IdCounter {
    private int count = 1;
    
    public String nextId() {
      String result = String.valueOf(count);
      ++count;
      
      return result;
    }
  }
  
  static boolean cachedMode;
  
  @BeforeClass
  public static void abstractBeforeClass() throws Exception {
    String oldCacheNamePropValue = System
        .getProperty("blockJoinParentFilterCache");
    
    cachedMode = random().nextBoolean();
    String newValue = cachedMode ? "blockJoinParentFilterCache" : "don't cache";
    
    System.setProperty("blockJoinParentFilterCache", newValue);
    if (oldCacheNamePropValue != null) {
      System.setProperty("blockJoinParentFilterCache", oldCacheNamePropValue);
    }
  }
}
