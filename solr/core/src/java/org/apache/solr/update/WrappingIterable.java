/**
 * 
 */
package org.apache.solr.update;

import java.util.Iterator;

/**
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

abstract class WrappingIterable<T> implements
    Iterable<T> {
  private final Iterable<T> inner;
  private boolean exausted = false;
  
  public WrappingIterable(Iterable<T>  block) {
    this.inner = block;
  }
  
  @Override
  public Iterator<T> iterator() {
    if(exausted){
      throw new IllegalStateException();
    }
    final Iterator<T> innerIter = inner.iterator();
    exausted = true;
    return new Iterator<T>() {

      @Override
      public boolean hasNext() {
        return innerIter.hasNext();
      }

      @Override
      public T next() {
        final T next = innerIter.next();
        onElem(next);
        return next;
      }


      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
      
    };
  }
  protected abstract void onElem(T next); 
  
}