---
title: "Batch Examples"
nav-title: Batch Examples
nav-parent_id: examples
nav-pos: 20
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

The following example programs showcase different applications of Flink from simple word counting to graph algorithms. The code samples illustrate the use of [Flink's DataSet API]({{ site.baseurl }}/dev/batch/index.html).

The full source code of the following and more examples can be found in the {% gh_link flink-examples/flink-examples-batch "flink-examples-batch" %} module of the Flink source repository.

- This will be replaced by the TOC {:toc}

## Running an example

In order to run a Flink example, we assume you have a running Flink instance available. The "Quickstart" and "Setup" tabs in the navigation describe various ways of starting Flink.

The easiest way is running the `./bin/start-cluster.sh`, which by default starts a local cluster with one JobManager and one TaskManager.

Each binary release of Flink contains an `examples` directory with jar files for each of the examples on this page.

To run the WordCount example, issue the following command:

{% highlight bash %} ./bin/flink run ./examples/batch/WordCount.jar {% endhighlight %}

The other examples can be started in a similar way.

Note that many examples run without passing any arguments for them, by using build-in data. To run WordCount with real data, you have to pass the path to the data:

{% highlight bash %} ./bin/flink run ./examples/batch/WordCount.jar --input /path/to/some/text/data --output /path/to/result {% endhighlight %}

Note that non-local file systems require a schema prefix, such as `hdfs://`.

## Word Count

WordCount is the "Hello World" of Big Data processing systems. It computes the frequency of words in a text collection. The algorithm works in two steps: First, the texts are splits the text to individual words. Second, the words are grouped and counted.

<div class="codetabs">
  <div data-lang="java">
    <p>
      {% highlight java %} ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    </p>
    
    <p>
      DataSet<string> text = env.readTextFile("/path/to/file");
    </p>
    
    <p>
      DataSet<Tuple2<String, Integer>> counts = // split up the lines in pairs (2-tuples) containing: (word,1) text.flatMap(new Tokenizer()) // group by the tuple field "0" and sum up tuple field "1" .groupBy(0) .sum(1);
    </p>
    
    <p>
      counts.writeAsCsv(outputPath, "\n", " ");
    </p>
    
    <p>
      // User-defined functions public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
    </p>
    
    <pre><code>@Override
public void flatMap(String value, Collector&lt;Tuple2&lt;String, Integer&gt;&gt; out) {
    // normalize and split the line
    String[] tokens = value.toLowerCase().split("\\W+");

    // emit the pairs
    for (String token : tokens) {
        if (token.length() &gt; 0) {
            out.collect(new Tuple2&lt;String, Integer&gt;(token, 1));
        }   
    }
}
</code></pre>
    
    <p>
      } {% endhighlight %}
    </p>
    
    <p>
      The {% gh_link /flink-examples/flink-examples-batch/src/main/java/org/apache/flink/examples/java/wordcount/WordCount.java "WordCount example" %} implements the above described algorithm with input parameters: <code>--input &lt;path&gt; --output &lt;path&gt;</code>. As test data, any text file will do.
    </p>
  </div>
  
  <div data-lang="scala">
    <p>
      {% highlight scala %} val env = ExecutionEnvironment.getExecutionEnvironment
    </p>
    
    <p>
      // get input data val text = env.readTextFile("/path/to/file")
    </p>
    
    <p>
      val counts = text.flatMap { <em>.toLowerCase.split("\\W+") filter { _.nonEmpty } } .map { (</em>, 1) } .groupBy(0) .sum(1)
    </p>
    
    <p>
      counts.writeAsCsv(outputPath, "\n", " ") {% endhighlight %}
    </p>
    
    <p>
      The {% gh_link /flink-examples/flink-examples-batch/src/main/scala/org/apache/flink/examples/scala/wordcount/WordCount.scala "WordCount example" %} implements the above described algorithm with input parameters: <code>--input &lt;path&gt; --output &lt;path&gt;</code>. As test data, any text file will do.
    </p>
  </div>
</div>

## Page Rank

The PageRank algorithm computes the "importance" of pages in a graph defined by links, which point from one pages to another page. It is an iterative graph algorithm, which means that it repeatedly applies the same computation. In each iteration, each page distributes its current rank over all its neighbors, and compute its new rank as a taxed sum of the ranks it received from its neighbors. The PageRank algorithm was popularized by the Google search engine which uses the importance of webpages to rank the results of search queries.

In this simple example, PageRank is implemented with a [bulk iteration](iterations.html) and a fixed number of iterations.

<div class="codetabs">
  <div data-lang="java">
    <p>
      {% highlight java %} ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    </p>
    
    <p>
      // read the pages and initial ranks by parsing a CSV file DataSet<Tuple2<Long, Double>> pagesWithRanks = env.readCsvFile(pagesInputPath) .types(Long.class, Double.class)
    </p>
    
    <p>
      // the links are encoded as an adjacency list: (page-id, Array(neighbor-ids)) DataSet<Tuple2<Long, Long[]>> pageLinkLists = getLinksDataSet(env);
    </p>
    
    <p>
      // set iterative data set IterativeDataSet<Tuple2<Long, Double>> iteration = pagesWithRanks.iterate(maxIterations);
    </p>
    
    <p>
      DataSet<Tuple2<Long, Double>> newRanks = iteration // join pages with outgoing edges and distribute rank .join(pageLinkLists).where(0).equalTo(0).flatMap(new JoinVertexWithEdgesMatch()) // collect and sum ranks .groupBy(0).sum(1) // apply dampening factor .map(new Dampener(DAMPENING_FACTOR, numPages));
    </p>
    
    <p>
      DataSet<Tuple2<Long, Double>> finalPageRanks = iteration.closeWith( newRanks, newRanks.join(iteration).where(0).equalTo(0) // termination condition .filter(new EpsilonFilter()));
    </p>
    
    <p>
      finalPageRanks.writeAsCsv(outputPath, "\n", " ");
    </p>
    
    <p>
      // User-defined functions
    </p>
    
    <p>
      public static final class JoinVertexWithEdgesMatch implements FlatJoinFunction<Tuple2<Long, Double>, Tuple2<Long, Long[]>, Tuple2<Long, Double>> {
    </p>
    
    <pre><code>@Override
public void join(&lt;Tuple2&lt;Long, Double&gt; page, Tuple2&lt;Long, Long[]&gt; adj,
                    Collector&lt;Tuple2&lt;Long, Double&gt;&gt; out) {
    Long[] neighbors = adj.f1;
    double rank = page.f1;
    double rankToDistribute = rank / ((double) neigbors.length);

    for (int i = 0; i &lt; neighbors.length; i++) {
        out.collect(new Tuple2&lt;Long, Double&gt;(neighbors[i], rankToDistribute));
    }
}
</code></pre>
    
    <p>
      }
    </p>
    
    <p>
      public static final class Dampener implements MapFunction<Tuple2<Long,Double>, Tuple2<Long,Double>> { private final double dampening, randomJump;
    </p>
    
    <pre><code>public Dampener(double dampening, double numVertices) {
    this.dampening = dampening;
    this.randomJump = (1 - dampening) / numVertices;
}

@Override
public Tuple2&lt;Long, Double&gt; map(Tuple2&lt;Long, Double&gt; value) {
    value.f1 = (value.f1 * dampening) + randomJump;
    return value;
}
</code></pre>
    
    <p>
      }
    </p>
    
    <p>
      public static final class EpsilonFilter implements FilterFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>>> {
    </p>
    
    <pre><code>@Override
public boolean filter(Tuple2&lt;Tuple2&lt;Long, Double&gt;, Tuple2&lt;Long, Double&gt;&gt; value) {
    return Math.abs(value.f0.f1 - value.f1.f1) &gt; EPSILON;
}
</code></pre>
    
    <p>
      } {% endhighlight %}
    </p>
    
    <p>
      The {% gh_link /flink-examples/flink-examples-batch/src/main/java/org/apache/flink/examples/java/graph/PageRank.java "PageRank program" %} implements the above example. It requires the following parameters to run: <code>--pages &lt;path&gt; --links &lt;path&gt; --output &lt;path&gt; --numPages &lt;n&gt; --iterations &lt;n&gt;</code>.
    </p>
  </div>
  
  <div data-lang="scala">
    <p>
      {% highlight scala %} // User-defined types case class Link(sourceId: Long, targetId: Long) case class Page(pageId: Long, rank: Double) case class AdjacencyList(sourceId: Long, targetIds: Array[Long])
    </p>
    
    <p>
      // set up execution environment val env = ExecutionEnvironment.getExecutionEnvironment
    </p>
    
    <p>
      // read the pages and initial ranks by parsing a CSV file val pages = env.readCsvFile<a href="pagesInputPath">Page</a>
    </p>
    
    <p>
      // the links are encoded as an adjacency list: (page-id, Array(neighbor-ids)) val links = env.readCsvFile<a href="linksInputPath">Link</a>
    </p>
    
    <p>
      // assign initial ranks to pages val pagesWithRanks = pages.map(p => Page(p, 1.0 / numPages))
    </p>
    
    <p>
      // build adjacency list from link input val adjacencyLists = links // initialize lists .map(e => AdjacencyList(e.sourceId, Array(e.targetId))) // concatenate lists .groupBy("sourceId").reduce { (l1, l2) => AdjacencyList(l1.sourceId, l1.targetIds ++ l2.targetIds) }
    </p>
    
    <p>
      // start iteration val finalRanks = pagesWithRanks.iterateWithTermination(maxIterations) { currentRanks => val newRanks = currentRanks // distribute ranks to target pages .join(adjacencyLists).where("pageId").equalTo("sourceId") { (page, adjacent, out: Collector[Page]) => for (targetId <- adjacent.targetids) { out.collect(page(targetid, page.rank > Page(p.pageId, (p.rank * DAMPENING_FACTOR) + ((1 - DAMPENING_FACTOR) / numPages)) }
    </p>
    
    <pre><code>// terminate if no rank update was significant
val termination = currentRanks.join(newRanks).where("pageId").equalTo("pageId") {
  (current, next, out: Collector[Int]) =&gt;
    // check for significant update
    if (math.abs(current.rank - next.rank) &gt; EPSILON) out.collect(1)
}

(newRanks, termination)
</code></pre>
    
    <p>
      }
    </p>
    
    <p>
      val result = finalRanks
    </p>
    
    <p>
      // emit result result.writeAsCsv(outputPath, "\n", " ") {% endhighlight %}
    </p>
    
    <p>
      The {% gh_link /flink-examples/flink-examples-batch/src/main/scala/org/apache/flink/examples/scala/graph/PageRankBasic.scala "PageRank program" %} implements the above example. It requires the following parameters to run: <code>--pages &lt;path&gt; --links &lt;path&gt; --output &lt;path&gt; --numPages &lt;n&gt; --iterations &lt;n&gt;</code>.
    </p>
  </div>
</div>

Input files are plain text files and must be formatted as follows: - Pages represented as an (long) ID separated by new-line characters. * For example `"1\n2\n12\n42\n63\n"` gives five pages with IDs 1, 2, 12, 42, and 63. - Links are represented as pairs of page IDs which are separated by space characters. Links are separated by new-line characters: * For example `"1 2\n2 12\n1 12\n42 63\n"` gives four (directed) links (1)->(2), (2)->(12), (1)->(12), and (42)->(63).

For this simple implementation it is required that each page has at least one incoming and one outgoing link (a page can point to itself).

## Connected Components

The Connected Components algorithm identifies parts of a larger graph which are connected by assigning all vertices in the same connected part the same component ID. Similar to PageRank, Connected Components is an iterative algorithm. In each step, each vertex propagates its current component ID to all its neighbors. A vertex accepts the component ID from a neighbor, if it is smaller than its own component ID.

This implementation uses a [delta iteration](iterations.html): Vertices that have not changed their component ID do not participate in the next step. This yields much better performance, because the later iterations typically deal only with a few outlier vertices.

<div class="codetabs">
  <div data-lang="java">
    <p>
      {% highlight java %} // read vertex and edge data DataSet<long> vertices = getVertexDataSet(env); DataSet<Tuple2<Long, Long>> edges = getEdgeDataSet(env).flatMap(new UndirectEdge());
    </p>
    
    <p>
      // assign the initial component IDs (equal to the vertex ID) DataSet<Tuple2<Long, Long>> verticesWithInitialId = vertices.map(new DuplicateValue<long>());
    </p>
    
    <p>
      // open a delta iteration DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration = verticesWithInitialId.iterateDelta(verticesWithInitialId, maxIterations, 0);
    </p>
    
    <p>
      // apply the step logic: DataSet<Tuple2<Long, Long>> changes = iteration.getWorkset() // join with the edges .join(edges).where(0).equalTo(0).with(new NeighborWithComponentIDJoin()) // select the minimum neighbor component ID .groupBy(0).aggregate(Aggregations.MIN, 1) // update if the component ID of the candidate is smaller .join(iteration.getSolutionSet()).where(0).equalTo(0) .flatMap(new ComponentIdFilter());
    </p>
    
    <p>
      // close the delta iteration (delta and new workset are identical) DataSet<Tuple2<Long, Long>> result = iteration.closeWith(changes, changes);
    </p>
    
    <p>
      // emit result result.writeAsCsv(outputPath, "\n", " ");
    </p>
    
    <p>
      // User-defined functions
    </p>
    
    <p>
      public static final class DuplicateValue<t> implements MapFunction<T, Tuple2<T, T>> {
    </p>
    
    <pre><code>@Override
public Tuple2&lt;T, T&gt; map(T vertex) {
    return new Tuple2&lt;T, T&gt;(vertex, vertex);
}
</code></pre>
    
    <p>
      }
    </p>
    
    <p>
      public static final class UndirectEdge implements FlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> { Tuple2<Long, Long> invertedEdge = new Tuple2<Long, Long>();
    </p>
    
    <pre><code>@Override
public void flatMap(Tuple2&lt;Long, Long&gt; edge, Collector&lt;Tuple2&lt;Long, Long&gt;&gt; out) {
    invertedEdge.f0 = edge.f1;
    invertedEdge.f1 = edge.f0;
    out.collect(edge);
    out.collect(invertedEdge);
}
</code></pre>
    
    <p>
      }
    </p>
    
    <p>
      public static final class NeighborWithComponentIDJoin implements JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {
    </p>
    
    <pre><code>@Override
public Tuple2&lt;Long, Long&gt; join(Tuple2&lt;Long, Long&gt; vertexWithComponent, Tuple2&lt;Long, Long&gt; edge) {
    return new Tuple2&lt;Long, Long&gt;(edge.f1, vertexWithComponent.f1);
}
</code></pre>
    
    <p>
      }
    </p>
    
    <p>
      public static final class ComponentIdFilter implements FlatMapFunction<Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>, Tuple2<Long, Long>> {
    </p>
    
    <pre><code>@Override
public void flatMap(Tuple2&lt;Tuple2&lt;Long, Long&gt;, Tuple2&lt;Long, Long&gt;&gt; value,
                    Collector&lt;Tuple2&lt;Long, Long&gt;&gt; out) {
    if (value.f0.f1 &lt; value.f1.f1) {
        out.collect(value.f0);
    }
}
</code></pre>
    
    <p>
      } {% endhighlight %}
    </p>
    
    <p>
      The {% gh_link /flink-examples/flink-examples-batch/src/main/java/org/apache/flink/examples/java/graph/ConnectedComponents.java "ConnectedComponents program" %} implements the above example. It requires the following parameters to run: <code>--vertices &lt;path&gt; --edges &lt;path&gt; --output &lt;path&gt; --iterations &lt;n&gt;</code>.
    </p>
  </div>
  
  <div data-lang="scala">
    <p>
      {% highlight scala %} // set up execution environment val env = ExecutionEnvironment.getExecutionEnvironment
    </p>
    
    <p>
      // read vertex and edge data // assign the initial components (equal to the vertex id) val vertices = getVerticesDataSet(env).map { id => (id, id) }
    </p>
    
    <p>
      // undirected edges by emitting for each input edge the input edges itself and an inverted // version val edges = getEdgesDataSet(env).flatMap { edge => Seq(edge, (edge._2, edge._1)) }
    </p>
    
    <p>
      // open a delta iteration val verticesWithComponents = vertices.iterateDelta(vertices, maxIterations, Array(0)) { (s, ws) =>
    </p>
    
    <pre><code>// apply the step logic: join with the edges
val allNeighbors = ws.join(edges).where(0).equalTo(0) { (vertex, edge) =&gt;
  (edge._2, vertex._2)
}

// select the minimum neighbor
val minNeighbors = allNeighbors.groupBy(0).min(1)

// update if the component of the candidate is smaller
val updatedComponents = minNeighbors.join(s).where(0).equalTo(0) {
  (newVertex, oldVertex, out: Collector[(Long, Long)]) =&gt;
    if (newVertex._2 &lt; oldVertex._2) out.collect(newVertex)
}

// delta and new workset are identical
(updatedComponents, updatedComponents)
</code></pre>
    
    <p>
      }
    </p>
    
    <p>
      verticesWithComponents.writeAsCsv(outputPath, "\n", " ")
    </p>
    
    <p>
      {% endhighlight %}
    </p>
    
    <p>
      The {% gh_link /flink-examples/flink-examples-batch/src/main/scala/org/apache/flink/examples/scala/graph/ConnectedComponents.scala "ConnectedComponents program" %} implements the above example. It requires the following parameters to run: <code>--vertices &lt;path&gt; --edges &lt;path&gt; --output &lt;path&gt; --iterations &lt;n&gt;</code>.
    </p>
  </div>
</div>

Input files are plain text files and must be formatted as follows: - Vertices represented as IDs and separated by new-line characters. * For example `"1\n2\n12\n42\n63\n"` gives five vertices with (1), (2), (12), (42), and (63). - Edges are represented as pairs for vertex IDs which are separated by space characters. Edges are separated by new-line characters: * For example `"1 2\n2 12\n1 12\n42 63\n"` gives four (undirected) links (1)-(2), (2)-(12), (1)-(12), and (42)-(63).

{% top %}