---
title: "Basic API Concepts"
nav-parent_id: dev
nav-pos: 1
nav-show_overview: true
nav-id: api-concepts
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

Flink programs are regular programs that implement transformations on distributed collections (e.g., filtering, mapping, updating state, joining, grouping, defining windows, aggregating). Collections are initially created from sources (e.g., by reading from files, kafka topics, or from local, in-memory collections). Results are returned via sinks, which may for example write the data to (distributed) files, or to standard output (for example, the command line terminal). Flink programs run in a variety of contexts, standalone, or embedded in other programs. The execution can happen in a local JVM, or on clusters of many machines.

Depending on the type of data sources, i.e. bounded or unbounded sources, you would either write a batch program or a streaming program where the DataSet API is used for batch and the DataStream API is used for streaming. This guide will introduce the basic concepts that are common to both APIs but please see our [Streaming Guide]({{ site.baseurl }}/dev/datastream_api.html) and [Batch Guide]({{ site.baseurl }}/dev/batch/index.html) for concrete information about writing programs with each API.

**NOTE:** When showing actual examples of how the APIs can be used we will use `StreamingExecutionEnvironment` and the `DataStream` API. The concepts are exactly the same in the `DataSet` API, just replace by `ExecutionEnvironment` and `DataSet`.

- This will be replaced by the TOC {:toc}

## DataSet and DataStream

Flink has the special classes `DataSet` and `DataStream` to represent data in a program. You can think of them as immutable collections of data that can contain duplicates. In the case of `DataSet` the data is finite while for a `DataStream` the number of elements can be unbounded.

These collections differ from regular Java collections in some key ways. First, they are immutable, meaning that once they are created you cannot add or remove elements. You can also not simply inspect the elements inside.

A collection is initially created by adding a source in a Flink program and new collections are derived from these by transforming them using API methods such as `map`, `filter` and so on.

## Anatomy of a Flink Program

Flink programs look like regular programs that transform collections of data. Each program consists of the same basic parts:

1. Obtain an `execution environment`,
2. Load/create the initial data,
3. Specify transformations on this data,
4. Specify where to put the results of your computations,
5. Trigger the program execution

<div class="codetabs">
  <div data-lang="java">
    <p>
      We will now give an overview of each of those steps, please refer to the respective sections for more details. Note that all core classes of the Java DataSet API are found in the package {% gh_link /flink-java/src/main/java/org/apache/flink/api/java "org.apache.flink.api.java" %} while the classes of the Java DataStream API can be found in {% gh_link /flink-streaming-java/src/main/java/org/apache/flink/streaming/api "org.apache.flink.streaming.api" %}.
    </p>
    
    <p>
      The <code>StreamExecutionEnvironment</code> is the basis for all Flink programs. You can obtain one using these static methods on <code>StreamExecutionEnvironment</code>:
    </p>
    
    <p>
      {% highlight java %} getExecutionEnvironment()
    </p>
    
    <p>
      createLocalEnvironment()
    </p>
    
    <p>
      createRemoteEnvironment(String host, int port, String... jarFiles) {% endhighlight %}
    </p>
    
    <p>
      Typically, you only need to use <code>getExecutionEnvironment()</code>, since this will do the right thing depending on the context: if you are executing your program inside an IDE or as a regular Java program it will create a local environment that will execute your program on your local machine. If you created a JAR file from your program, and invoke it through the <a href="{{ site.baseurl }}/ops/cli.html">command line</a>, the Flink cluster manager will execute your main method and <code>getExecutionEnvironment()</code> will return an execution environment for executing your program on a cluster.
    </p>
    
    <p>
      For specifying data sources the execution environment has several methods to read from files using various methods: you can just read them line by line, as CSV files, or using completely custom data input formats. To just read a text file as a sequence of lines, you can use:
    </p>
    
    <p>
      {% highlight java %} final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    </p>
    
    <p>
      DataStream<string> text = env.readTextFile("file:///path/to/file"); {% endhighlight %}
    </p>
    
    <p>
      This will give you a DataStream on which you can then apply transformations to create new derived DataStreams.
    </p>
    
    <p>
      You apply transformations by calling methods on DataStream with a transformation functions. For example, a map transformation looks like this:
    </p>
    
    <p>
      {% highlight java %} DataStream<string> input = ...;
    </p>
    
    <p>
      DataStream<integer> parsed = input.map(new MapFunction<String, Integer>() { @Override public Integer map(String value) { return Integer.parseInt(value); } }); {% endhighlight %}
    </p>
    
    <p>
      This will create a new DataStream by converting every String in the original collection to an Integer.
    </p>
    
    <p>
      Once you have a DataStream containing your final results, you can write it to an outside system by creating a sink. These are just some example methods for creating a sink:
    </p>
    
    <p>
      {% highlight java %} writeAsText(String path)
    </p>
    
    <p>
      print() {% endhighlight %}
    </p>
  </div>
  
  <div data-lang="scala">
    <p>
      We will now give an overview of each of those steps, please refer to the respective sections for more details. Note that all core classes of the Scala DataSet API are found in the package {% gh_link /flink-scala/src/main/scala/org/apache/flink/api/scala "org.apache.flink.api.scala" %} while the classes of the Scala DataStream API can be found in {% gh_link /flink-streaming-scala/src/main/scala/org/apache/flink/streaming/api/scala "org.apache.flink.streaming.api.scala" %}.
    </p>
    
    <p>
      The <code>StreamExecutionEnvironment</code> is the basis for all Flink programs. You can obtain one using these static methods on <code>StreamExecutionEnvironment</code>:
    </p>
    
    <p>
      {% highlight scala %} getExecutionEnvironment()
    </p>
    
    <p>
      createLocalEnvironment()
    </p>
    
    <p>
      createRemoteEnvironment(host: String, port: Int, jarFiles: String*) {% endhighlight %}
    </p>
    
    <p>
      Typically, you only need to use <code>getExecutionEnvironment()</code>, since this will do the right thing depending on the context: if you are executing your program inside an IDE or as a regular Java program it will create a local environment that will execute your program on your local machine. If you created a JAR file from your program, and invoke it through the <a href="{{ site.baseurl }}/ops/cli.html">command line</a>, the Flink cluster manager will execute your main method and <code>getExecutionEnvironment()</code> will return an execution environment for executing your program on a cluster.
    </p>
    
    <p>
      For specifying data sources the execution environment has several methods to read from files using various methods: you can just read them line by line, as CSV files, or using completely custom data input formats. To just read a text file as a sequence of lines, you can use:
    </p>
    
    <p>
      {% highlight scala %} val env = StreamExecutionEnvironment.getExecutionEnvironment()
    </p>
    
    <p>
      val text: DataStream[String] = env.readTextFile("file:///path/to/file") {% endhighlight %}
    </p>
    
    <p>
      This will give you a DataStream on which you can then apply transformations to create new derived DataStreams.
    </p>
    
    <p>
      You apply transformations by calling methods on DataSet with a transformation functions. For example, a map transformation looks like this:
    </p>
    
    <p>
      {% highlight scala %} val input: DataSet[String] = ...
    </p>
    
    <p>
      val mapped = input.map { x => x.toInt } {% endhighlight %}
    </p>
    
    <p>
      This will create a new DataStream by converting every String in the original collection to an Integer.
    </p>
    
    <p>
      Once you have a DataStream containing your final results, you can write it to an outside system by creating a sink. These are just some example methods for creating a sink:
    </p>
    
    <p>
      {% highlight scala %} writeAsText(path: String)
    </p>
    
    <p>
      print() {% endhighlight %}
    </p>
  </div>
</div>

Once you specified the complete program you need to **trigger the program execution** by calling `execute()` on the `StreamExecutionEnvironment`. Depending on the type of the `ExecutionEnvironment` the execution will be triggered on your local machine or submit your program for execution on a cluster.

The `execute()` method is returning a `JobExecutionResult`, this contains execution times and accumulator results.

Please see the [Streaming Guide]({{ site.baseurl }}/dev/datastream_api.html) for information about streaming data sources and sink and for more in-depths information about the supported transformations on DataStream.

Check out the [Batch Guide]({{ site.baseurl }}/dev/batch/index.html) for information about batch data sources and sink and for more in-depths information about the supported transformations on DataSet.

{% top %}

## Lazy Evaluation

All Flink programs are executed lazily: When the program's main method is executed, the data loading and transformations do not happen directly. Rather, each operation is created and added to the program's plan. The operations are actually executed when the execution is explicitly triggered by an `execute()` call on the execution environment. Whether the program is executed locally or on a cluster depends on the type of execution environment

The lazy evaluation lets you construct sophisticated programs that Flink executes as one holistically planned unit.

{% top %}

## Specifying Keys

Some transformations (join, coGroup, keyBy, groupBy) require that a key be defined on a collection of elements. Other transformations (Reduce, GroupReduce, Aggregate, Windows) allow data being grouped on a key before they are applied.

A DataSet is grouped as {% highlight java %} DataSet<...> input = // [...] DataSet<...> reduced = input .groupBy(/*define key here*/) .reduceGroup(/*do something*/); {% endhighlight %}

while a key can be specified on a DataStream using {% highlight java %} DataStream<...> input = // [...] DataStream<...> windowed = input .keyBy(/*define key here*/) .window(/*window specification*/); {% endhighlight %}

The data model of Flink is not based on key-value pairs. Therefore, you do not need to physically pack the data set types into keys and values. Keys are "virtual": they are defined as functions over the actual data to guide the grouping operator.

**NOTE:** In the following discussion we will use the `DataStream` API and `keyBy`. For the DataSet API you just have to replace by `DataSet` and `groupBy`.

### Define keys for Tuples

{:.no_toc}

The simplest case is grouping Tuples on one or more fields of the Tuple:

<div class="codetabs">
  <div data-lang="java">
    <p>
      {% highlight java %} DataStream<Tuple3<Integer,String,Long>> input = // [...] KeyedStream<Tuple3<Integer,String,Long>,Tuple> keyed = input.keyBy(0) {% endhighlight %}
    </p>
  </div>
  
  <div data-lang="scala">
    <p>
      {% highlight scala %} val input: DataStream[(Int, String, Long)] = // [...] val keyed = input.keyBy(0) {% endhighlight %}
    </p>
  </div>
</div>

The tuples are grouped on the first field (the one of Integer type).

<div class="codetabs">
  <div data-lang="java">
    <p>
      {% highlight java %} DataStream<Tuple3<Integer,String,Long>> input = // [...] KeyedStream<Tuple3<Integer,String,Long>,Tuple> keyed = input.keyBy(0,1) {% endhighlight %}
    </p>
  </div>
  
  <div data-lang="scala">
    <p>
      {% highlight scala %} val input: DataSet[(Int, String, Long)] = // [...] val grouped = input.groupBy(0,1) {% endhighlight %}
    </p>
  </div>
</div>

Here, we group the tuples on a composite key consisting of the first and the second field.

A note on nested Tuples: If you have a DataStream with a nested tuple, such as:

{% highlight java %} DataStream<Tuple3<Tuple2<Integer, Float>,String,Long>> ds; {% endhighlight %}

Specifying `keyBy(0)` will cause the system to use the full `Tuple2` as a key (with the Integer and Float being the key). If you want to "navigate" into the nested `Tuple2`, you have to use field expression keys which are explained below.

### Define keys using Field Expressions

{:.no_toc}

You can use String-based field expressions to reference nested fields and define keys for grouping, sorting, joining, or coGrouping.

Field expressions make it very easy to select fields in (nested) composite types such as [Tuple](#tuples-and-case-classes) and [POJO](#pojos) types.

<div class="codetabs">
  <div data-lang="java">
    <p>
      In the example below, we have a <code>WC</code> POJO with two fields "word" and "count". To group by the field <code>word</code>, we just pass its name to the <code>keyBy()</code> function. {% highlight java %} // some ordinary POJO (Plain old Java Object) public class WC { public String word; public int count; } DataStream<wc> words = // [...] DataStream<wc> wordCounts = words.keyBy("word").window(/<em>window specification</em>/); {% endhighlight %}
    </p>
    
    <p>
      <strong>Field Expression Syntax</strong>:
    </p>
    
    <ul>
      <li>
        <p>
          Select POJO fields by their field name. For example <code>"user"</code> refers to the "user" field of a POJO type.
        </p>
      </li>
      <li>
        <p>
          Select Tuple fields by their field name or 0-offset field index. For example <code>"f0"</code> and <code>"5"</code> refer to the first and sixth field of a Java Tuple type, respectively.
        </p>
      </li>
      <li>
        <p>
          You can select nested fields in POJOs and Tuples. For example <code>"user.zip"</code> refers to the "zip" field of a POJO which is stored in the "user" field of a POJO type. Arbitrary nesting and mixing of POJOs and Tuples is supported such as <code>"f1.user.zip"</code> or <code>"user.f3.1.zip"</code>.
        </p>
      </li>
      <li>
        <p>
          You can select the full type using the <code>"*"</code> wildcard expressions. This does also work for types which are not Tuple or POJO types.
        </p>
      </li>
    </ul>
    
    <p>
      <strong>Field Expression Example</strong>:
    </p>
    
    <p>
      {% highlight java %} public static class WC { public ComplexNestedClass complex; //nested POJO private int count; // getter / setter for private field (count) public int getCount() { return count; } public void setCount(int c) { this.count = c; } } public static class ComplexNestedClass { public Integer someNumber; public float someFloat; public Tuple3<Long, Long, String> word; public IntWritable hadoopCitizen; } {% endhighlight %}
    </p>
    
    <p>
      These are valid field expressions for the example code above:
    </p>
    
    <ul>
      <li>
        <p>
          <code>"count"</code>: The count field in the <code>WC</code> class.
        </p>
      </li>
      <li>
        <p>
          <code>"complex"</code>: Recursively selects all fields of the field complex of POJO type <code>ComplexNestedClass</code>.
        </p>
      </li>
      <li>
        <p>
          <code>"complex.word.f2"</code>: Selects the last field of the nested <code>Tuple3</code>.
        </p>
      </li>
      <li>
        <p>
          <code>"complex.hadoopCitizen"</code>: Selects the Hadoop <code>IntWritable</code> type.
        </p>
      </li>
    </ul>
  </div>
  
  <div data-lang="scala">
    <p>
      In the example below, we have a <code>WC</code> POJO with two fields "word" and "count". To group by the field <code>word</code>, we just pass its name to the <code>keyBy()</code> function. {% highlight java %} // some ordinary POJO (Plain old Java Object) class WC(var word: String, var count: Int) { def this() { this("", 0L) } } val words: DataStream[WC] = // [...] val wordCounts = words.keyBy("word").window(/<em>window specification</em>/)
    </p>
    
    <p>
      // or, as a case class, which is less typing case class WC(word: String, count: Int) val words: DataStream[WC] = // [...] val wordCounts = words.keyBy("word").window(/<em>window specification</em>/) {% endhighlight %}
    </p>
    
    <p>
      <strong>Field Expression Syntax</strong>:
    </p>
    
    <ul>
      <li>
        <p>
          Select POJO fields by their field name. For example <code>"user"</code> refers to the "user" field of a POJO type.
        </p>
      </li>
      <li>
        <p>
          Select Tuple fields by their 1-offset field name or 0-offset field index. For example <code>"_1"</code> and <code>"5"</code> refer to the first and sixth field of a Scala Tuple type, respectively.
        </p>
      </li>
      <li>
        <p>
          You can select nested fields in POJOs and Tuples. For example <code>"user.zip"</code> refers to the "zip" field of a POJO which is stored in the "user" field of a POJO type. Arbitrary nesting and mixing of POJOs and Tuples is supported such as <code>"_2.user.zip"</code> or <code>"user._4.1.zip"</code>.
        </p>
      </li>
      <li>
        <p>
          You can select the full type using the <code>"_"</code> wildcard expressions. This does also work for types which are not Tuple or POJO types.
        </p>
      </li>
    </ul>
    
    <p>
      <strong>Field Expression Example</strong>:
    </p>
    
    <p>
      {% highlight scala %} class WC(var complex: ComplexNestedClass, var count: Int) { def this() { this(null, 0) } }
    </p>
    
    <p>
      class ComplexNestedClass( var someNumber: Int, someFloat: Float, word: (Long, Long, String), hadoopCitizen: IntWritable) { def this() { this(0, 0, (0, 0, ""), new IntWritable(0)) } } {% endhighlight %}
    </p>
    
    <p>
      These are valid field expressions for the example code above:
    </p>
    
    <ul>
      <li>
        <p>
          <code>"count"</code>: The count field in the <code>WC</code> class.
        </p>
      </li>
      <li>
        <p>
          <code>"complex"</code>: Recursively selects all fields of the field complex of POJO type <code>ComplexNestedClass</code>.
        </p>
      </li>
      <li>
        <p>
          <code>"complex.word._3"</code>: Selects the last field of the nested <code>Tuple3</code>.
        </p>
      </li>
      <li>
        <p>
          <code>"complex.hadoopCitizen"</code>: Selects the Hadoop <code>IntWritable</code> type.
        </p>
      </li>
    </ul>
  </div>
</div>

### Define keys using Key Selector Functions

{:.no_toc}

An additional way to define keys are "key selector" functions. A key selector function takes a single element as input and returns the key for the element. The key can be of any type and be derived from deterministic computations.

The following example shows a key selector function that simply returns the field of an object:

<div class="codetabs">
  <div data-lang="java">
    <p>
      {% highlight java %} // some ordinary POJO public class WC {public String word; public int count;} DataStream<wc> words = // [...] KeyedStream<wc> keyed = words .keyBy(new KeySelector<WC, String>() { public String getKey(WC wc) { return wc.word; } }); {% endhighlight %}
    </p>
  </div>
  
  <div data-lang="scala">
    <p>
      {% highlight scala %} // some ordinary case class case class WC(word: String, count: Int) val words: DataStream[WC] = // [...] val keyed = words.keyBy( _.word ) {% endhighlight %}
    </p>
  </div>
</div>

{% top %}

## Specifying Transformation Functions

Most transformations require user-defined functions. This section lists different ways of how they can be specified

<div class="codetabs">
  <div data-lang="java">
    <h4>
      Implementing an interface
    </h4>
    
    <p>
      The most basic way is to implement one of the provided interfaces:
    </p>
    
    <p>
      {% highlight java %} class MyMapFunction implements MapFunction<String, Integer> { public Integer map(String value) { return Integer.parseInt(value); } }; data.map(new MyMapFunction()); {% endhighlight %}
    </p>
    
    <h4>
      Anonymous classes
    </h4>
    
    <p>
      You can pass a function as an anonymous class: {% highlight java %} data.map(new MapFunction<String, Integer> () { public Integer map(String value) { return Integer.parseInt(value); } }); {% endhighlight %}
    </p>
    
    <h4>
      Java 8 Lambdas
    </h4>
    
    <p>
      Flink also supports Java 8 Lambdas in the Java API.
    </p>
    
    <p>
      {% highlight java %} data.filter(s -> s.startsWith("http://")); {% endhighlight %}
    </p>
    
    <p>
      {% highlight java %} data.reduce((i1,i2) -> i1 + i2); {% endhighlight %}
    </p>
    
    <h4>
      Rich functions
    </h4>
    
    <p>
      All transformations that require a user-defined function can instead take as argument a <em>rich</em> function. For example, instead of
    </p>
    
    <p>
      {% highlight java %} class MyMapFunction implements MapFunction<String, Integer> { public Integer map(String value) { return Integer.parseInt(value); } }; {% endhighlight %}
    </p>
    
    <p>
      you can write
    </p>
    
    <p>
      {% highlight java %} class MyMapFunction extends RichMapFunction<String, Integer> { public Integer map(String value) { return Integer.parseInt(value); } }; {% endhighlight %}
    </p>
    
    <p>
      and pass the function as usual to a <code>map</code> transformation:
    </p>
    
    <p>
      {% highlight java %} data.map(new MyMapFunction()); {% endhighlight %}
    </p>
    
    <p>
      Rich functions can also be defined as an anonymous class: {% highlight java %} data.map (new RichMapFunction<String, Integer>() { public Integer map(String value) { return Integer.parseInt(value); } }); {% endhighlight %}
    </p>
  </div>
  
  <div data-lang="scala">
    <h4>
      Lambda Functions
    </h4>
    
    <p>
      As already seen in previous examples all operations accept lambda functions for describing the operation: {% highlight scala %} val data: DataSet[String] = // [...] data.filter { _.startsWith("http://") } {% endhighlight %}
    </p>
    
    <p>
      {% highlight scala %} val data: DataSet[Int] = // [...] data.reduce { (i1,i2) => i1 + i2 } // or data.reduce { _ + _ } {% endhighlight %}
    </p>
    
    <h4>
      Rich functions
    </h4>
    
    <p>
      All transformations that take as argument a lambda function can instead take as argument a <em>rich</em> function. For example, instead of
    </p>
    
    <p>
      {% highlight scala %} data.map { x => x.toInt } {% endhighlight %}
    </p>
    
    <p>
      you can write
    </p>
    
    <p>
      {% highlight scala %} class MyMapFunction extends RichMapFunction[String, Int] { def map(in: String):Int = { in.toInt } }; {% endhighlight %}
    </p>
    
    <p>
      and pass the function to a <code>map</code> transformation:
    </p>
    
    <p>
      {% highlight scala %} data.map(new MyMapFunction()) {% endhighlight %}
    </p>
    
    <p>
      Rich functions can also be defined as an anonymous class: {% highlight scala %} data.map (new RichMapFunction[String, Int] { def map(in: String):Int = { in.toInt } }) {% endhighlight %}
    </p>
  </div>
</div>

Rich functions provide, in addition to the user-defined function (map, reduce, etc), four methods: `open`, `close`, `getRuntimeContext`, and `setRuntimeContext`. These are useful for parameterizing the function (see [Passing Parameters to Functions]({{ site.baseurl }}/dev/batch/index.html#passing-parameters-to-functions)), creating and finalizing local state, accessing broadcast variables (see [Broadcast Variables]({{ site.baseurl }}/dev/batch/index.html#broadcast-variables)), and for accessing runtime information such as accumulators and counters (see [Accumulators and Counters](#accumulators--counters)), and information on iterations (see [Iterations]({{ site.baseurl }}/dev/batch/iterations.html)).

{% top %}

## Supported Data Types

Flink places some restrictions on the type of elements that can be in a DataSet or DataStream. The reason for this is that the system analyzes the types to determine efficient execution strategies.

There are six different categories of data types:

1. **Java Tuples** and **Scala Case Classes**
2. **Java POJOs**
3. **Primitive Types**
4. **Regular Classes**
5. **Values**
6. **Hadoop Writables**
7. **Special Types**

#### Tuples and Case Classes

<div class="codetabs">
  <div data-lang="java">
    <p>
      Tuples are composite types that contain a fixed number of fields with various types. The Java API provides classes from <code>Tuple1</code> up to <code>Tuple25</code>. Every field of a tuple can be an arbitrary Flink type including further tuples, resulting in nested tuples. Fields of a tuple can be accessed directly using the field's name as <code>tuple.f4</code>, or using the generic getter method <code>tuple.getField(int position)</code>. The field indices start at 0. Note that this stands in contrast to the Scala tuples, but it is more consistent with Java's general indexing.
    </p>
    
    <p>
      {% highlight java %} DataStream<Tuple2<String, Integer>> wordCounts = env.fromElements( new Tuple2<String, Integer>("hello", 1), new Tuple2<String, Integer>("world", 2));
    </p>
    
    <p>
      wordCounts.map(new MapFunction<Tuple2<String, Integer>, Integer>() { @Override public Integer map(Tuple2<String, Integer> value) throws Exception { return value.f1; } });
    </p>
    
    <p>
      wordCounts.keyBy(0); // also valid .keyBy("f0")
    </p>
    
    <p>
      {% endhighlight %}
    </p>
  </div>
  
  <div data-lang="scala">
    <p>
      Scala case classes (and Scala tuples which are a special case of case classes), are composite types that contain a fixed number of fields with various types. Tuple fields are addressed by their 1-offset names such as <code>_1</code> for the first field. Case class fields are accessed by their name.
    </p>
    
    <p>
      {% highlight scala %} case class WordCount(word: String, count: Int) val input = env.fromElements( WordCount("hello", 1), WordCount("world", 2)) // Case Class Data Set
    </p>
    
    <p>
      input.keyBy("word")// key by field expression "word"
    </p>
    
    <p>
      val input2 = env.fromElements(("hello", 1), ("world", 2)) // Tuple2 Data Set
    </p>
    
    <p>
      input2.keyBy(0, 1) // key by field positions 0 and 1 {% endhighlight %}
    </p>
  </div>
</div>

#### POJOs

Java and Scala classes are treated by Flink as a special POJO data type if they fulfill the following requirements:

- The class must be public.

- It must have a public constructor without arguments (default constructor).

- All fields are either public or must be accessible through getter and setter functions. For a field called `foo` the getter and setter methods must be named `getFoo()` and `setFoo()`.

- The type of a field must be supported by Flink. At the moment, Flink uses [Avro](http://avro.apache.org) to serialize arbitrary objects (such as `Date`).

Flink analyzes the structure of POJO types, i.e., it learns about the fields of a POJO. As a result POJO types are easier to use than general types. Moreover, Flink can process POJOs more efficiently than general types.

The following example shows a simple POJO with two public fields.

<div class="codetabs">
  <div data-lang="java">
    <p>
      {% highlight java %} public class WordWithCount {
    </p>
    
    <pre><code>public String word;
public int count;

public WordWithCount() {}

public WordWithCount(String word, int count) {
    this.word = word;
    this.count = count;
}
</code></pre>
    
    <p>
      }
    </p>
    
    <p>
      DataStream<wordwithcount> wordCounts = env.fromElements( new WordWithCount("hello", 1), new WordWithCount("world", 2));
    </p>
    
    <p>
      wordCounts.keyBy("word"); // key by field expression "word"
    </p>
    
    <p>
      {% endhighlight %}
    </p>
  </div>
  
  <div data-lang="scala">
    <p>
      {% highlight scala %} class WordWithCount(var word: String, var count: Int) { def this() { this(null, -1) } }
    </p>
    
    <p>
      val input = env.fromElements( new WordWithCount("hello", 1), new WordWithCount("world", 2)) // Case Class Data Set
    </p>
    
    <p>
      input.keyBy("word")// key by field expression "word"
    </p>
    
    <p>
      {% endhighlight %}
    </p>
  </div>
</div>

#### Primitive Types

Flink supports all Java and Scala primitive types such as `Integer`, `String`, and `Double`.

#### General Class Types

Flink supports most Java and Scala classes (API and custom). Restrictions apply to classes containing fields that cannot be serialized, like file pointers, I/O streams, or other native resources. Classes that follow the Java Beans conventions work well in general.

All classes that are not identified as POJO types (see POJO requirements above) are handled by Flink as general class types. Flink treats these data types as black boxes and is not able to access their content (i.e., for efficient sorting). General types are de/serialized using the serialization framework [Kryo](https://github.com/EsotericSoftware/kryo).

#### Values

*Value* types describe their serialization and deserialization manually. Instead of going through a general purpose serialization framework, they provide custom code for those operations by means of implementing the `org.apache.flinktypes.Value` interface with the methods `read` and `write`. Using a Value type is reasonable when general purpose serialization would be highly inefficient. An example would be a data type that implements a sparse vector of elements as an array. Knowing that the array is mostly zero, one can use a special encoding for the non-zero elements, while the general purpose serialization would simply write all array elements.

The `org.apache.flinktypes.CopyableValue` interface supports manual internal cloning logic in a similar way.

Flink comes with pre-defined Value types that correspond to basic data types. (`ByteValue`, `ShortValue`, `IntValue`, `LongValue`, `FloatValue`, `DoubleValue`, `StringValue`, `CharValue`, `BooleanValue`). These Value types act as mutable variants of the basic data types: Their value can be altered, allowing programmers to reuse objects and take pressure off the garbage collector.

#### Hadoop Writables

You can use types that implement the `org.apache.hadoop.Writable` interface. The serialization logic defined in the `write()`and `readFields()` methods will be used for serialization.

#### Special Types

You can use special types, including Scala's `Either`, `Option`, and `Try`. The Java API has its own custom implementation of `Either`. Similarly to Scala's `Either`, it represents a value of one two possible types, *Left* or *Right*. `Either` can be useful for error handling or operators that need to output two different types of records.

#### Type Erasure & Type Inference

*Note: This Section is only relevant for Java.*

The Java compiler throws away much of the generic type information after compilation. This is known as *type erasure* in Java. It means that at runtime, an instance of an object does not know its generic type any more. For example, instances of `DataStream<String>` and `DataStream<Long>` look the same to the JVM.

Flink requires type information at the time when it prepares the program for execution (when the main method of the program is called). The Flink Java API tries to reconstruct the type information that was thrown away in various ways and store it explicitly in the data sets and operators. You can retrieve the type via `DataStream.getType()`. The method returns an instance of `TypeInformation`, which is Flink's internal way of representing types.

The type inference has its limits and needs the "cooperation" of the programmer in some cases. Examples for that are methods that create data sets from collections, such as `ExecutionEnvironment.fromCollection(),` where you can pass an argument that describes the type. But also generic functions like `MapFunction<I, O>` may need extra type information.

The {% gh_link /flink-core/src/main/java/org/apache/flink/api/java/typeutils/ResultTypeQueryable.java "ResultTypeQueryable" %} interface can be implemented by input formats and functions to tell the API explicitly about their return type. The *input types* that the functions are invoked with can usually be inferred by the result types of the previous operations.

{% top %}

## Accumulators & Counters

Accumulators are simple constructs with an **add operation** and a **final accumulated result**, which is available after the job ended.

The most straightforward accumulator is a **counter**: You can increment it using the ```Accumulator.add(V value)``` method. At the end of the job Flink will sum up (merge) all partial results and send the result to the client. Accumulators are useful during debugging or if you quickly want to find out more about your data.

Flink currently has the following **built-in accumulators**. Each of them implements the {% gh_link /flink-core/src/main/java/org/apache/flink/api/common/accumulators/Accumulator.java "Accumulator" %} interface.

- {% gh_link /flink-core/src/main/java/org/apache/flink/api/common/accumulators/IntCounter.java "**IntCounter**" %}, {% gh_link /flink-core/src/main/java/org/apache/flink/api/common/accumulators/LongCounter.java "**LongCounter**" %} and {% gh_link /flink-core/src/main/java/org/apache/flink/api/common/accumulators/DoubleCounter.java "**DoubleCounter**" %}: See below for an example using a counter.
- {% gh_link /flink-core/src/main/java/org/apache/flink/api/common/accumulators/Histogram.java "**Histogram**" %}: A histogram implementation for a discrete number of bins. Internally it is just a map from Integer to Integer. You can use this to compute distributions of values, e.g. the distribution of words-per-line for a word count program.

**How to use accumulators:**

First you have to create an accumulator object (here a counter) in the user-defined transformation function where you want to use it.

{% highlight java %} private IntCounter numLines = new IntCounter(); {% endhighlight %}

Second you have to register the accumulator object, typically in the ```open()``` method of the *rich* function. Here you also define the name.

{% highlight java %} getRuntimeContext().addAccumulator("num-lines", this.numLines); {% endhighlight %}

You can now use the accumulator anywhere in the operator function, including in the ```open()``` and ```close()``` methods.

{% highlight java %} this.numLines.add(1); {% endhighlight %}

The overall result will be stored in the ```JobExecutionResult``` object which is returned from the `execute()` method of the execution environment (currently this only works if the execution waits for the completion of the job).

{% highlight java %} myJobExecutionResult.getAccumulatorResult("num-lines") {% endhighlight %}

All accumulators share a single namespace per job. Thus you can use the same accumulator in different operator functions of your job. Flink will internally merge all accumulators with the same name.

A note on accumulators and iterations: Currently the result of accumulators is only available after the overall job has ended. We plan to also make the result of the previous iteration available in the next iteration. You can use {% gh_link /flink-java/src/main/java/org/apache/flink/api/java/operators/IterativeDataSet.java#L98 "Aggregators" %} to compute per-iteration statistics and base the termination of iterations on such statistics.

**Custom accumulators:**

To implement your own accumulator you simply have to write your implementation of the Accumulator interface. Feel free to create a pull request if you think your custom accumulator should be shipped with Flink.

You have the choice to implement either {% gh_link /flink-core/src/main/java/org/apache/flink/api/common/accumulators/Accumulator.java "Accumulator" %} or {% gh_link /flink-core/src/main/java/org/apache/flink/api/common/accumulators/SimpleAccumulator.java "SimpleAccumulator" %}.

    Accumulator<V,R> is most flexible: It defines a type ```V``` for the value to add, and a result type ```R``` for the final result. E.g. for a histogram, ```V``` is a number and ```R``` is a histogram. ```SimpleAccumulator``` is for the cases where both types are the same, e.g. for counters.

{% top %}