#/**
# * Licensed to the Apache Software Foundation (ASF) under one or more
# * contributor license agreements.  See the NOTICE file distributed with
# * this work for additional information regarding copyright ownership.
# * The ASF licenses this file to You under the Apache License, Version 2.0
# * (the "License"); you may not use this file except in compliance with
# * the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */
# -------------------------------------------------------------------------------------
# multi val params are iterated by NewRound's, added to reports, start with column name.

analyzer=org.apache.lucene.analysis.standard.StandardAnalyzer
directory=FSDirectory

doc.stored=true
doc.tokenized=true
doc.term.vector=false
log.step=10000
docs.dir=reuters-out

search.num.hits=1000

query.maker=org.apache.lucene.benchmark.byTask.feeds.FileBasedQueryMaker
file.query.maker.file=/home/perf/projects/solr-fork/lucene/benchmark/conf/10K-wiki-body-terms.txt

# task at this depth or less would print when they start
task.max.depth.log=2

# -------------------------------------------------------------------------------------

{ "Rounds"
    OpenReader
    { "Warmup" Search > : 20000
    [
      { "SearchSameRdr" Search > : 100000
    ]:1
    CloseReader
}
RepSumByNameRound