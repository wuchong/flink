---
title: Graph API
nav-parent_id: graphs
nav-pos: 1
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

* This will be replaced by the TOC {:toc}

## Graph Representation

In Gelly, a `Graph` is represented by a `DataSet` of vertices and a `DataSet` of edges.

The `Graph` nodes are represented by the `Vertex` type. A `Vertex` is defined by a unique ID and a value. `Vertex` IDs should implement the `Comparable` interface. Vertices without value can be represented by setting the value type to `NullValue`.

<div class="codetabs">
  <div data-lang="java">
    <p>
      {% highlight java %} // create a new vertex with a Long ID and a String value Vertex<Long, String> v = new Vertex<Long, String>(1L, "foo");
    </p>
    
    <p>
      // create a new vertex with a Long ID and no value Vertex<Long, NullValue> v = new Vertex<Long, NullValue>(1L, NullValue.getInstance()); {% endhighlight %}
    </p>
  </div>
  
  <div data-lang="scala">
    <p>
      {% highlight scala %} // create a new vertex with a Long ID and a String value val v = new Vertex(1L, "foo")
    </p>
    
    <p>
      // create a new vertex with a Long ID and no value val v = new Vertex(1L, NullValue.getInstance()) {% endhighlight %}
    </p>
  </div>
</div>

The graph edges are represented by the `Edge` type. An `Edge` is defined by a source ID (the ID of the source `Vertex`), a target ID (the ID of the target `Vertex`) and an optional value. The source and target IDs should be of the same type as the `Vertex` IDs. Edges with no value have a `NullValue` value type.

<div class="codetabs">
  <div data-lang="java">
    <p>
      {% highlight java %} Edge<Long, Double> e = new Edge<Long, Double>(1L, 2L, 0.5);
    </p>
    
    <p>
      // reverse the source and target of this edge Edge<Long, Double> reversed = e.reverse();
    </p>
    
    <p>
      Double weight = e.getValue(); // weight = 0.5 {% endhighlight %}
    </p>
  </div>
  
  <div data-lang="scala">
    <p>
      {% highlight scala %} val e = new Edge(1L, 2L, 0.5)
    </p>
    
    <p>
      // reverse the source and target of this edge val reversed = e.reverse
    </p>
    
    <p>
      val weight = e.getValue // weight = 0.5 {% endhighlight %}
    </p>
  </div>
</div>

In Gelly an `Edge` is always directed from the source vertex to the target vertex. A `Graph` may be undirected if for every `Edge` it contains a matching `Edge` from the target vertex to the source vertex.

{% top %}

## Graph Creation

You can create a `Graph` in the following ways:

* from a `DataSet` of edges and an optional `DataSet` of vertices:

<div class="codetabs">
  <div data-lang="java">
    <p>
      {% highlight java %} ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    </p>
    
    <p>
      DataSet<Vertex<String, Long>> vertices = ...
    </p>
    
    <p>
      DataSet<Edge<String, Double>> edges = ...
    </p>
    
    <p>
      Graph<String, Long, Double> graph = Graph.fromDataSet(vertices, edges, env); {% endhighlight %}
    </p>
  </div>
  
  <div data-lang="scala">
    <p>
      {% highlight scala %} val env = ExecutionEnvironment.getExecutionEnvironment
    </p>
    
    <p>
      val vertices: DataSet[Vertex[String, Long]] = ...
    </p>
    
    <p>
      val edges: DataSet[Edge[String, Double]] = ...
    </p>
    
    <p>
      val graph = Graph.fromDataSet(vertices, edges, env) {% endhighlight %}
    </p>
  </div>
</div>

* from a `DataSet` of `Tuple2` representing the edges. Gelly will convert each `Tuple2` to an `Edge`, where the first field will be the source ID and the second field will be the target ID. Both vertex and edge values will be set to `NullValue`.

<div class="codetabs">
  <div data-lang="java">
    <p>
      {% highlight java %} ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    </p>
    
    <p>
      DataSet<Tuple2<String, String>> edges = ...
    </p>
    
    <p>
      Graph<String, NullValue, NullValue> graph = Graph.fromTuple2DataSet(edges, env); {% endhighlight %}
    </p>
  </div>
  
  <div data-lang="scala">
    <p>
      {% highlight scala %} val env = ExecutionEnvironment.getExecutionEnvironment
    </p>
    
    <p>
      val edges: DataSet[(String, String)] = ...
    </p>
    
    <p>
      val graph = Graph.fromTuple2DataSet(edges, env) {% endhighlight %}
    </p>
  </div>
</div>

* from a `DataSet` of `Tuple3` and an optional `DataSet` of `Tuple2`. In this case, Gelly will convert each `Tuple3` to an `Edge`, where the first field will be the source ID, the second field will be the target ID and the third field will be the edge value. Equivalently, each `Tuple2` will be converted to a `Vertex`, where the first field will be the vertex ID and the second field will be the vertex value:

<div class="codetabs">
  <div data-lang="java">
    <p>
      {% highlight java %} ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    </p>
    
    <p>
      DataSet<Tuple2<String, Long>> vertexTuples = env.readCsvFile("path/to/vertex/input").types(String.class, Long.class);
    </p>
    
    <p>
      DataSet<Tuple3<String, String, Double>> edgeTuples = env.readCsvFile("path/to/edge/input").types(String.class, String.class, Double.class);
    </p>
    
    <p>
      Graph<String, Long, Double> graph = Graph.fromTupleDataSet(vertexTuples, edgeTuples, env); {% endhighlight %}
    </p>
    
    <ul>
      <li>
        <p>
          from a CSV file of Edge data and an optional CSV file of Vertex data. In this case, Gelly will convert each row from the Edge CSV file to an <code>Edge</code>, where the first field will be the source ID, the second field will be the target ID and the third field (if present) will be the edge value. Equivalently, each row from the optional Vertex CSV file will be converted to a <code>Vertex</code>, where the first field will be the vertex ID and the second field (if present) will be the vertex value. In order to get a <code>Graph</code> from a <code>GraphCsvReader</code> one has to specify the types, using one of the following methods:
        </p>
      </li>
      <li>
        <p>
          <code>types(Class&lt;K&gt; vertexKey, Class&lt;VV&gt; vertexValue,Class&lt;EV&gt; edgeValue)</code>: both vertex and edge values are present.
        </p>
      </li>
      <li>
        <code>edgeTypes(Class&lt;K&gt; vertexKey, Class&lt;EV&gt; edgeValue)</code>: the Graph has edge values, but no vertex values.
      </li>
      <li>
        <code>vertexTypes(Class&lt;K&gt; vertexKey, Class&lt;VV&gt; vertexValue)</code>: the Graph has vertex values, but no edge values.
      </li>
      <li>
        <code>keyType(Class&lt;K&gt; vertexKey)</code>: the Graph has no vertex values and no edge values.
      </li>
    </ul>
    
    <p>
      {% highlight java %} ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    </p>
    
    <p>
      // create a Graph with String Vertex IDs, Long Vertex values and Double Edge values Graph<String, Long, Double> graph = Graph.fromCsvReader("path/to/vertex/input", "path/to/edge/input", env) .types(String.class, Long.class, Double.class);
    </p>
    
    <p>
      // create a Graph with neither Vertex nor Edge values Graph<Long, NullValue, NullValue> simpleGraph = Graph.fromCsvReader("path/to/edge/input", env).keyType(Long.class); {% endhighlight %}
    </p>
  </div>
  
  <div data-lang="scala">
    <p>
      {% highlight scala %} val env = ExecutionEnvironment.getExecutionEnvironment
    </p>
    
    <p>
      val vertexTuples = env.readCsvFile<a href="&quot;path/to/vertex/input&quot;">String, Long</a>
    </p>
    
    <p>
      val edgeTuples = env.readCsvFile<a href="&quot;path/to/edge/input&quot;">String, String, Double</a>
    </p>
    
    <p>
      val graph = Graph.fromTupleDataSet(vertexTuples, edgeTuples, env) {% endhighlight %}
    </p>
    
    <ul>
      <li>
        from a CSV file of Edge data and an optional CSV file of Vertex data. In this case, Gelly will convert each row from the Edge CSV file to an <code>Edge</code>. The first field of the each row will be the source ID, the second field will be the target ID and the third field (if present) will be the edge value. If the edges have no associated value, set the edge value type parameter (3rd type argument) to <code>NullValue</code>. You can also specify that the vertices are initialized with a vertex value. If you provide a path to a CSV file via <code>pathVertices</code>, each row of this file will be converted to a <code>Vertex</code>. The first field of each row will be the vertex ID and the second field will be the vertex value. If you provide a vertex value initializer <code>MapFunction</code> via the <code>vertexValueInitializer</code> parameter, then this function is used to generate the vertex values. The set of vertices will be created automatically from the edges input. If the vertices have no associated value, set the vertex value type parameter (2nd type argument) to <code>NullValue</code>. The vertices will then be automatically created from the edges input with vertex value of type <code>NullValue</code>.
      </li>
    </ul>
    
    <p>
      {% highlight scala %} val env = ExecutionEnvironment.getExecutionEnvironment
    </p>
    
    <p>
      // create a Graph with String Vertex IDs, Long Vertex values and Double Edge values val graph = Graph.fromCsvReader[String, Long, Double]( pathVertices = "path/to/vertex/input", pathEdges = "path/to/edge/input", env = env)
    </p>
    
    <p>
      // create a Graph with neither Vertex nor Edge values val simpleGraph = Graph.fromCsvReader[Long, NullValue, NullValue]( pathEdges = "path/to/edge/input", env = env)
    </p>
    
    <p>
      // create a Graph with Double Vertex values generated by a vertex value initializer and no Edge values val simpleGraph = Graph.fromCsvReader[Long, Double, NullValue]( pathEdges = "path/to/edge/input", vertexValueInitializer = new MapFunction<a href="">Long, Double</a> { def map(id: Long): Double = { id.toDouble } }, env = env) {% endhighlight %}
    </p>
  </div>
</div>

* from a `Collection` of edges and an optional `Collection` of vertices:

<div class="codetabs">
  <div data-lang="java">
    <p>
      {% highlight java %} ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    </p>
    
    <p>
      List<Vertex<Long, Long>> vertexList = new ArrayList...
    </p>
    
    <p>
      List<Edge<Long, String>> edgeList = new ArrayList...
    </p>
    
    <p>
      Graph<Long, Long, String> graph = Graph.fromCollection(vertexList, edgeList, env); {% endhighlight %}
    </p>
    
    <p>
      If no vertex input is provided during Graph creation, Gelly will automatically produce the <code>Vertex</code> <code>DataSet</code> from the edge input. In this case, the created vertices will have no values. Alternatively, you can provide a <code>MapFunction</code> as an argument to the creation method, in order to initialize the <code>Vertex</code> values:
    </p>
    
    <p>
      {% highlight java %} ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    </p>
    
    <p>
      // initialize the vertex value to be equal to the vertex ID Graph<Long, Long, String> graph = Graph.fromCollection(edgeList, new MapFunction<Long, Long>() { public Long map(Long value) { return value; } }, env); {% endhighlight %}
    </p>
  </div>
  
  <div data-lang="scala">
    <p>
      {% highlight scala %} val env = ExecutionEnvironment.getExecutionEnvironment
    </p>
    
    <p>
      val vertexList = List(...)
    </p>
    
    <p>
      val edgeList = List(...)
    </p>
    
    <p>
      val graph = Graph.fromCollection(vertexList, edgeList, env) {% endhighlight %}
    </p>
    
    <p>
      If no vertex input is provided during Graph creation, Gelly will automatically produce the <code>Vertex</code> <code>DataSet</code> from the edge input. In this case, the created vertices will have no values. Alternatively, you can provide a <code>MapFunction</code> as an argument to the creation method, in order to initialize the <code>Vertex</code> values:
    </p>
    
    <p>
      {% highlight java %} val env = ExecutionEnvironment.getExecutionEnvironment
    </p>
    
    <p>
      // initialize the vertex value to be equal to the vertex ID val graph = Graph.fromCollection(edgeList, new MapFunction[Long, Long] { def map(id: Long): Long = id }, env) {% endhighlight %}
    </p>
  </div>
</div>

{% top %}

## Graph Properties

Gelly includes the following methods for retrieving various Graph properties and metrics:

<div class="codetabs">
  <div data-lang="java">
    <p>
      {% highlight java %} // get the Vertex DataSet DataSet<Vertex<K, VV>> getVertices()
    </p>
    
    <p>
      // get the Edge DataSet DataSet<Edge<K, EV>> getEdges()
    </p>
    
    <p>
      // get the IDs of the vertices as a DataSet DataSet<k> getVertexIds()
    </p>
    
    <p>
      // get the source-target pairs of the edge IDs as a DataSet DataSet<Tuple2<K, K>> getEdgeIds()
    </p>
    
    <p>
      // get a DataSet of <vertex id, in-degree> pairs for all vertices DataSet<Tuple2<K, LongValue>> inDegrees()
    </p>
    
    <p>
      // get a DataSet of <vertex id, out-degree> pairs for all vertices DataSet<Tuple2<K, LongValue>> outDegrees()
    </p>
    
    <p>
      // get a DataSet of <vertex id, degree> pairs for all vertices, where degree is the sum of in- and out- degrees DataSet<Tuple2<K, LongValue>> getDegrees()
    </p>
    
    <p>
      // get the number of vertices long numberOfVertices()
    </p>
    
    <p>
      // get the number of edges long numberOfEdges()
    </p>
    
    <p>
      // get a DataSet of Triplets<srcVertex, trgVertex, edge> DataSet<Triplet<K, VV, EV>> getTriplets()
    </p>
    
    <p>
      {% endhighlight %}
    </p>
  </div>
  
  <div data-lang="scala">
    <p>
      {% highlight scala %} // get the Vertex DataSet getVertices: DataSet[Vertex[K, VV]]
    </p>
    
    <p>
      // get the Edge DataSet getEdges: DataSet[Edge[K, EV]]
    </p>
    
    <p>
      // get the IDs of the vertices as a DataSet getVertexIds: DataSet[K]
    </p>
    
    <p>
      // get the source-target pairs of the edge IDs as a DataSet getEdgeIds: DataSet[(K, K)]
    </p>
    
    <p>
      // get a DataSet of <vertex id, in-degree> pairs for all vertices inDegrees: DataSet[(K, LongValue)]
    </p>
    
    <p>
      // get a DataSet of <vertex id, out-degree> pairs for all vertices outDegrees: DataSet[(K, LongValue)]
    </p>
    
    <p>
      // get a DataSet of <vertex id, degree> pairs for all vertices, where degree is the sum of in- and out- degrees getDegrees: DataSet[(K, LongValue)]
    </p>
    
    <p>
      // get the number of vertices numberOfVertices: Long
    </p>
    
    <p>
      // get the number of edges numberOfEdges: Long
    </p>
    
    <p>
      // get a DataSet of Triplets<srcVertex, trgVertex, edge> getTriplets: DataSet[Triplet[K, VV, EV]]
    </p>
    
    <p>
      {% endhighlight %}
    </p>
  </div>
</div>

{% top %}

## Graph Transformations

* **Map**: Gelly provides specialized methods for applying a map transformation on the vertex values or edge values. `mapVertices` and `mapEdges` return a new `Graph`, where the IDs of the vertices (or edges) remain unchanged, while the values are transformed according to the provided user-defined map function. The map functions also allow changing the type of the vertex or edge values.

<div class="codetabs">
  <div data-lang="java">
    <p>
      {% highlight java %} ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment(); Graph<Long, Long, Long> graph = Graph.fromDataSet(vertices, edges, env);
    </p>
    
    <p>
      // increment each vertex value by one Graph<Long, Long, Long> updatedGraph = graph.mapVertices( new MapFunction<Vertex<Long, Long>, Long>() { public Long map(Vertex<Long, Long> value) { return value.getValue() + 1; } }); {% endhighlight %}
    </p>
  </div>
  
  <div data-lang="scala">
    <p>
      {% highlight scala %} val env = ExecutionEnvironment.getExecutionEnvironment val graph = Graph.fromDataSet(vertices, edges, env)
    </p>
    
    <p>
      // increment each vertex value by one val updatedGraph = graph.mapVertices(v => v.getValue + 1) {% endhighlight %}
    </p>
  </div>
</div>

* **Translate**: Gelly provides specialized methods for translating the value and/or type of vertex and edge IDs (`translateGraphIDs`), vertex values (`translateVertexValues`), or edge values (`translateEdgeValues`). Translation is performed by the user-defined map function, several of which are provided in the `org.apache.flink.graph.asm.translate` package. The same `MapFunction` can be used for all the three translate methods.

<div class="codetabs">
  <div data-lang="java">
    <p>
      {% highlight java %} ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment(); Graph<Long, Long, Long> graph = Graph.fromDataSet(vertices, edges, env);
    </p>
    
    <p>
      // translate each vertex and edge ID to a String Graph<String, Long, Long> updatedGraph = graph.translateGraphIds( new MapFunction<Long, String>() { public String map(Long id) { return id.toString(); } });
    </p>
    
    <p>
      // translate vertex IDs, edge IDs, vertex values, and edge values to LongValue Graph<LongValue, LongValue, LongValue> updatedGraph = graph .translateGraphIds(new LongToLongValue()) .translateVertexValues(new LongToLongValue()) .translateEdgeValues(new LongToLongValue()) {% endhighlight %}
    </p>
  </div>
  
  <div data-lang="scala">
    <p>
      {% highlight scala %} val env = ExecutionEnvironment.getExecutionEnvironment val graph = Graph.fromDataSet(vertices, edges, env)
    </p>
    
    <p>
      // translate each vertex and edge ID to a String val updatedGraph = graph.translateGraphIds(id => id.toString) {% endhighlight %}
    </p>
  </div>
</div>

* **Filter**: A filter transformation applies a user-defined filter function on the vertices or edges of the `Graph`. `filterOnEdges` will create a sub-graph of the original graph, keeping only the edges that satisfy the provided predicate. Note that the vertex dataset will not be modified. Respectively, `filterOnVertices` applies a filter on the vertices of the graph. Edges whose source and/or target do not satisfy the vertex predicate are removed from the resulting edge dataset. The `subgraph` method can be used to apply a filter function to the vertices and the edges at the same time.

<div class="codetabs">
  <div data-lang="java">
    <p>
      {% highlight java %} Graph<Long, Long, Long> graph = ...
    </p>
    
    <p>
      graph.subgraph( new FilterFunction<Vertex<Long, Long>>() { public boolean filter(Vertex<Long, Long> vertex) { // keep only vertices with positive values return (vertex.getValue() > 0); } }, new FilterFunction<Edge<Long, Long>>() { public boolean filter(Edge<Long, Long> edge) { // keep only edges with negative values return (edge.getValue() < 0); } }) {% endhighlight %}
    </p>
  </div>
  
  <div data-lang="scala">
    <p>
      {% highlight scala %} val graph: Graph[Long, Long, Long] = ...
    </p>
    
    <p>
      // keep only vertices with positive values // and only edges with negative values graph.subgraph((vertex => vertex.getValue > 0), (edge => edge.getValue < 0)) {% endhighlight %}
    </p>
  </div>
</div>

<p class="text-center">
    <img alt="Filter Transformations" width="80%" src="{{ site.baseurl }}/fig/gelly-filter.png"/>
</p>

* **Join**: Gelly provides specialized methods for joining the vertex and edge datasets with other input datasets. `joinWithVertices` joins the vertices with a `Tuple2` input data set. The join is performed using the vertex ID and the first field of the `Tuple2` input as the join keys. The method returns a new `Graph` where the vertex values have been updated according to a provided user-defined transformation function. Similarly, an input dataset can be joined with the edges, using one of three methods. `joinWithEdges` expects an input `DataSet` of `Tuple3` and joins on the composite key of both source and target vertex IDs. `joinWithEdgesOnSource` expects a `DataSet` of `Tuple2` and joins on the source key of the edges and the first attribute of the input dataset and `joinWithEdgesOnTarget` expects a `DataSet` of `Tuple2` and joins on the target key of the edges and the first attribute of the input dataset. All three methods apply a transformation function on the edge and the input data set values. Note that if the input dataset contains a key multiple times, all Gelly join methods will only consider the first value encountered.

<div class="codetabs">
  <div data-lang="java">
    <p>
      {% highlight java %} Graph<Long, Double, Double> network = ...
    </p>
    
    <p>
      DataSet<Tuple2<Long, LongValue>> vertexOutDegrees = network.outDegrees();
    </p>
    
    <p>
      // assign the transition probabilities as the edge weights Graph<Long, Double, Double> networkWithWeights = network.joinWithEdgesOnSource(vertexOutDegrees, new VertexJoinFunction<Double, LongValue>() { public Double vertexJoin(Double vertexValue, LongValue inputValue) { return vertexValue / inputValue.getValue(); } }); {% endhighlight %}
    </p>
  </div>
  
  <div data-lang="scala">
    <p>
      {% highlight scala %} val network: Graph[Long, Double, Double] = ...
    </p>
    
    <p>
      val vertexOutDegrees: DataSet[(Long, LongValue)] = network.outDegrees
    </p>
    
    <p>
      // assign the transition probabilities as the edge weights val networkWithWeights = network.joinWithEdgesOnSource(vertexOutDegrees, (v1: Double, v2: LongValue) => v1 / v2.getValue) {% endhighlight %}
    </p>
  </div>
</div>

* **Reverse**: the `reverse()` method returns a new `Graph` where the direction of all edges has been reversed.

* **Undirected**: In Gelly, a `Graph` is always directed. Undirected graphs can be represented by adding all opposite-direction edges to a graph. For this purpose, Gelly provides the `getUndirected()` method.

* **Union**: Gelly's `union()` method performs a union operation on the vertex and edge sets of the specified graph and the current graph. Duplicate vertices are removed from the resulting `Graph`, while if duplicate edges exist, these will be preserved.

<p class="text-center">
    <img alt="Union Transformation" width="50%" src="{{ site.baseurl }}/fig/gelly-union.png"/>
</p>

* **Difference**: Gelly's `difference()` method performs a difference on the vertex and edge sets of the current graph and the specified graph.

* **Intersect**: Gelly's `intersect()` method performs an intersect on the edge sets of the current graph and the specified graph. The result is a new `Graph` that contains all edges that exist in both input graphs. Two edges are considered equal, if they have the same source identifier, target identifier and edge value. Vertices in the resulting graph have no value. If vertex values are required, one can for example retrieve them from one of the input graphs using the `joinWithVertices()` method. Depending on the parameter `distinct`, equal edges are either contained once in the resulting `Graph` or as often as there are pairs of equal edges in the input graphs.

<div class="codetabs">
  <div data-lang="java">
    <p>
      {% highlight java %} ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    </p>
    
    <p>
      // create first graph from edges {(1, 3, 12) (1, 3, 13), (1, 3, 13)} List<Edge<Long, Long>> edges1 = ... Graph<Long, NullValue, Long> graph1 = Graph.fromCollection(edges1, env);
    </p>
    
    <p>
      // create second graph from edges {(1, 3, 13)} List<Edge<Long, Long>> edges2 = ... Graph<Long, NullValue, Long> graph2 = Graph.fromCollection(edges2, env);
    </p>
    
    <p>
      // Using distinct = true results in {(1,3,13)} Graph<Long, NullValue, Long> intersect1 = graph1.intersect(graph2, true);
    </p>
    
    <p>
      // Using distinct = false results in {(1,3,13),(1,3,13)} as there is one edge pair Graph<Long, NullValue, Long> intersect2 = graph1.intersect(graph2, false);
    </p>
    
    <p>
      {% endhighlight %}
    </p>
  </div>
  
  <div data-lang="scala">
    <p>
      {% highlight scala %} val env = ExecutionEnvironment.getExecutionEnvironment
    </p>
    
    <p>
      // create first graph from edges {(1, 3, 12) (1, 3, 13), (1, 3, 13)} val edges1: List[Edge[Long, Long]] = ... val graph1 = Graph.fromCollection(edges1, env)
    </p>
    
    <p>
      // create second graph from edges {(1, 3, 13)} val edges2: List[Edge[Long, Long]] = ... val graph2 = Graph.fromCollection(edges2, env)
    </p>
    
    <p>
      // Using distinct = true results in {(1,3,13)} val intersect1 = graph1.intersect(graph2, true)
    </p>
    
    <p>
      // Using distinct = false results in {(1,3,13),(1,3,13)} as there is one edge pair val intersect2 = graph1.intersect(graph2, false) {% endhighlight %}
    </p>
  </div>
</div>

-{% top %}

## Graph Mutations

Gelly includes the following methods for adding and removing vertices and edges from an input `Graph`:

<div class="codetabs">
  <div data-lang="java">
    <p>
      {% highlight java %} // adds a Vertex to the Graph. If the Vertex already exists, it will not be added again. Graph<K, VV, EV> addVertex(final Vertex<K, VV> vertex)
    </p>
    
    <p>
      // adds a list of vertices to the Graph. If the vertices already exist in the graph, they will not be added once more. Graph<K, VV, EV> addVertices(List<Vertex<K, VV>> verticesToAdd)
    </p>
    
    <p>
      // adds an Edge to the Graph. If the source and target vertices do not exist in the graph, they will also be added. Graph<K, VV, EV> addEdge(Vertex<K, VV> source, Vertex<K, VV> target, EV edgeValue)
    </p>
    
    <p>
      // adds a list of edges to the Graph. When adding an edge for a non-existing set of vertices, the edge is considered invalid and ignored. Graph<K, VV, EV> addEdges(List<Edge<K, EV>> newEdges)
    </p>
    
    <p>
      // removes the given Vertex and its edges from the Graph. Graph<K, VV, EV> removeVertex(Vertex<K, VV> vertex)
    </p>
    
    <p>
      // removes the given list of vertices and their edges from the Graph Graph<K, VV, EV> removeVertices(List<Vertex<K, VV>> verticesToBeRemoved)
    </p>
    
    <p>
      // removes <em>all</em> edges that match the given Edge from the Graph. Graph<K, VV, EV> removeEdge(Edge<K, EV> edge)
    </p>
    
    <p>
      // removes <em>all</em> edges that match the edges in the given list Graph<K, VV, EV> removeEdges(List<Edge<K, EV>> edgesToBeRemoved) {% endhighlight %}
    </p>
  </div>
  
  <div data-lang="scala">
    <p>
      {% highlight scala %} // adds a Vertex to the Graph. If the Vertex already exists, it will not be added again. addVertex(vertex: Vertex[K, VV])
    </p>
    
    <p>
      // adds a list of vertices to the Graph. If the vertices already exist in the graph, they will not be added once more. addVertices(verticesToAdd: List[Vertex[K, VV]])
    </p>
    
    <p>
      // adds an Edge to the Graph. If the source and target vertices do not exist in the graph, they will also be added. addEdge(source: Vertex[K, VV], target: Vertex[K, VV], edgeValue: EV)
    </p>
    
    <p>
      // adds a list of edges to the Graph. When adding an edge for a non-existing set of vertices, the edge is considered invalid and ignored. addEdges(edges: List[Edge[K, EV]])
    </p>
    
    <p>
      // removes the given Vertex and its edges from the Graph. removeVertex(vertex: Vertex[K, VV])
    </p>
    
    <p>
      // removes the given list of vertices and their edges from the Graph removeVertices(verticesToBeRemoved: List[Vertex[K, VV]])
    </p>
    
    <p>
      // removes <em>all</em> edges that match the given Edge from the Graph. removeEdge(edge: Edge[K, EV])
    </p>
    
    <p>
      // removes <em>all</em> edges that match the edges in the given list removeEdges(edgesToBeRemoved: List[Edge[K, EV]]) {% endhighlight %}
    </p>
  </div>
</div>

## Neighborhood Methods

Neighborhood methods allow vertices to perform an aggregation on their first-hop neighborhood. `reduceOnEdges()` can be used to compute an aggregation on the values of the neighboring edges of a vertex and `reduceOnNeighbors()` can be used to compute an aggregation on the values of the neighboring vertices. These methods assume associative and commutative aggregations and exploit combiners internally, significantly improving performance. The neighborhood scope is defined by the `EdgeDirection` parameter, which takes the values `IN`, `OUT` or `ALL`. `IN` will gather all in-coming edges (neighbors) of a vertex, `OUT` will gather all out-going edges (neighbors), while `ALL` will gather all edges (neighbors).

For example, assume that you want to select the minimum weight of all out-edges for each vertex in the following graph:

<p class="text-center">
    <img alt="reduceOnEdges Example" width="50%" src="{{ site.baseurl }}/fig/gelly-example-graph.png"/>
</p>

The following code will collect the out-edges for each vertex and apply the `SelectMinWeight()` user-defined function on each of the resulting neighborhoods:

<div class="codetabs">
  <div data-lang="java">
    <p>
      {% highlight java %} Graph<Long, Long, Double> graph = ...
    </p>
    
    <p>
      DataSet<Tuple2<Long, Double>> minWeights = graph.reduceOnEdges(new SelectMinWeight(), EdgeDirection.OUT);
    </p>
    
    <p>
      // user-defined function to select the minimum weight static final class SelectMinWeight implements ReduceEdgesFunction<double> {
    </p>
    
    <pre><code>    @Override
    public Double reduceEdges(Double firstEdgeValue, Double secondEdgeValue) {
        return Math.min(firstEdgeValue, secondEdgeValue);
    }
</code></pre>
    
    <p>
      } {% endhighlight %}
    </p>
  </div>
  
  <div data-lang="scala">
    <p>
      {% highlight scala %} val graph: Graph[Long, Long, Double] = ...
    </p>
    
    <p>
      val minWeights = graph.reduceOnEdges(new SelectMinWeight, EdgeDirection.OUT)
    </p>
    
    <p>
      // user-defined function to select the minimum weight final class SelectMinWeight extends ReduceEdgesFunction[Double] { override def reduceEdges(firstEdgeValue: Double, secondEdgeValue: Double): Double = { Math.min(firstEdgeValue, secondEdgeValue) } } {% endhighlight %}
    </p>
  </div>
</div>

<p class="text-center">
    <img alt="reduceOnEdges Example" width="50%" src="{{ site.baseurl }}/fig/gelly-reduceOnEdges.png"/>
</p>

Similarly, assume that you would like to compute the sum of the values of all in-coming neighbors, for every vertex. The following code will collect the in-coming neighbors for each vertex and apply the `SumValues()` user-defined function on each neighborhood:

<div class="codetabs">
  <div data-lang="java">
    <p>
      {% highlight java %} Graph<Long, Long, Double> graph = ...
    </p>
    
    <p>
      DataSet<Tuple2<Long, Long>> verticesWithSum = graph.reduceOnNeighbors(new SumValues(), EdgeDirection.IN);
    </p>
    
    <p>
      // user-defined function to sum the neighbor values static final class SumValues implements ReduceNeighborsFunction<long> {
    </p>
    
    <pre><code>        @Override
        public Long reduceNeighbors(Long firstNeighbor, Long secondNeighbor) {
            return firstNeighbor + secondNeighbor;
    }
</code></pre>
    
    <p>
      } {% endhighlight %}
    </p>
  </div>
  
  <div data-lang="scala">
    <p>
      {% highlight scala %} val graph: Graph[Long, Long, Double] = ...
    </p>
    
    <p>
      val verticesWithSum = graph.reduceOnNeighbors(new SumValues, EdgeDirection.IN)
    </p>
    
    <p>
      // user-defined function to sum the neighbor values final class SumValues extends ReduceNeighborsFunction[Long] { override def reduceNeighbors(firstNeighbor: Long, secondNeighbor: Long): Long = { firstNeighbor + secondNeighbor } } {% endhighlight %}
    </p>
  </div>
</div>

<p class="text-center">
    <img alt="reduceOnNeighbors Example" width="70%" src="{{ site.baseurl }}/fig/gelly-reduceOnNeighbors.png"/>
</p>

When the aggregation function is not associative and commutative or when it is desirable to return more than one values per vertex, one can use the more general `groupReduceOnEdges()` and `groupReduceOnNeighbors()` methods. These methods return zero, one or more values per vertex and provide access to the whole neighborhood.

For example, the following code will output all the vertex pairs which are connected with an edge having a weight of 0.5 or more:

<div class="codetabs">
  <div data-lang="java">
    <p>
      {% highlight java %} Graph<Long, Long, Double> graph = ...
    </p>
    
    <p>
      DataSet<Tuple2<Vertex<Long, Long>, Vertex<Long, Long>>> vertexPairs = graph.groupReduceOnNeighbors(new SelectLargeWeightNeighbors(), EdgeDirection.OUT);
    </p>
    
    <p>
      // user-defined function to select the neighbors which have edges with weight > 0.5 static final class SelectLargeWeightNeighbors implements NeighborsFunctionWithVertexValue<Long, Long, Double, Tuple2<Vertex<Long, Long>, Vertex<Long, Long>>> {
    </p>
    
    <pre><code>    @Override
    public void iterateNeighbors(Vertex&lt;Long, Long&gt; vertex,
            Iterable&lt;Tuple2&lt;Edge&lt;Long, Double&gt;, Vertex&lt;Long, Long&gt;&gt;&gt; neighbors,
            Collector&lt;Tuple2&lt;Vertex&lt;Long, Long&gt;, Vertex&lt;Long, Long&gt;&gt;&gt; out) {

        for (Tuple2&lt;Edge&lt;Long, Double&gt;, Vertex&lt;Long, Long&gt;&gt; neighbor : neighbors) {
            if (neighbor.f0.f2 &gt; 0.5) {
                out.collect(new Tuple2&lt;Vertex&lt;Long, Long&gt;, Vertex&lt;Long, Long&gt;&gt;(vertex, neighbor.f1));
            }
        }
    }
</code></pre>
    
    <p>
      } {% endhighlight %}
    </p>
  </div>
  
  <div data-lang="scala">
    <p>
      {% highlight scala %} val graph: Graph[Long, Long, Double] = ...
    </p>
    
    <p>
      val vertexPairs = graph.groupReduceOnNeighbors(new SelectLargeWeightNeighbors, EdgeDirection.OUT)
    </p>
    
    <p>
      // user-defined function to select the neighbors which have edges with weight > 0.5 final class SelectLargeWeightNeighbors extends NeighborsFunctionWithVertexValue[Long, Long, Double, (Vertex[Long, Long], Vertex[Long, Long])] {
    </p>
    
    <pre><code>override def iterateNeighbors(vertex: Vertex[Long, Long],
    neighbors: Iterable[(Edge[Long, Double], Vertex[Long, Long])],
    out: Collector[(Vertex[Long, Long], Vertex[Long, Long])]) = {

        for (neighbor &lt;- neighbors) {
            if (neighbor._1.getValue() &gt; 0.5) {
                out.collect(vertex, neighbor._2)
            }
        }
    }
</code></pre>
    
    <p>
      } {% endhighlight %}
    </p>
  </div>
</div>

When the aggregation computation does not require access to the vertex value (for which the aggregation is performed), it is advised to use the more efficient `EdgesFunction` and `NeighborsFunction` for the user-defined functions. When access to the vertex value is required, one should use `EdgesFunctionWithVertexValue` and `NeighborsFunctionWithVertexValue` instead.

{% top %}

## Graph Validation

Gelly provides a simple utility for performing validation checks on input graphs. Depending on the application context, a graph may or may not be valid according to certain criteria. For example, a user might need to validate whether their graph contains duplicate edges or whether its structure is bipartite. In order to validate a graph, one can define a custom `GraphValidator` and implement its `validate()` method. `InvalidVertexIdsValidator` is Gelly's pre-defined validator. It checks that the edge set contains valid vertex IDs, i.e. that all edge IDs also exist in the vertex IDs set.

<div class="codetabs">
  <div data-lang="java">
    <p>
      {% highlight java %} ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    </p>
    
    <p>
      // create a list of vertices with IDs = {1, 2, 3, 4, 5} List<Vertex<Long, Long>> vertices = ...
    </p>
    
    <p>
      // create a list of edges with IDs = {(1, 2) (1, 3), (2, 4), (5, 6)} List<Edge<Long, Long>> edges = ...
    </p>
    
    <p>
      Graph<Long, Long, Long> graph = Graph.fromCollection(vertices, edges, env);
    </p>
    
    <p>
      // will return false: 6 is an invalid ID graph.validate(new InvalidVertexIdsValidator<Long, Long, Long>());
    </p>
    
    <p>
      {% endhighlight %}
    </p>
  </div>
  
  <div data-lang="scala">
    <p>
      {% highlight scala %} val env = ExecutionEnvironment.getExecutionEnvironment
    </p>
    
    <p>
      // create a list of vertices with IDs = {1, 2, 3, 4, 5} val vertices: List[Vertex[Long, Long]] = ...
    </p>
    
    <p>
      // create a list of edges with IDs = {(1, 2) (1, 3), (2, 4), (5, 6)} val edges: List[Edge[Long, Long]] = ...
    </p>
    
    <p>
      val graph = Graph.fromCollection(vertices, edges, env)
    </p>
    
    <p>
      // will return false: 6 is an invalid ID graph.validate(new InvalidVertexIdsValidator[Long, Long, Long])
    </p>
    
    <p>
      {% endhighlight %}
    </p>
  </div>
</div>

{% top %}