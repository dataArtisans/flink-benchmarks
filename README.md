# flink-benchmarks

This repository contains sets of micro benchmarks designed to run on single machine to help 
[Apache Flink's](https://github.com/apache/flink) developers assess performance implications of 
their changes. 

The main methods defined in the various classes (test cases) are using [jmh](http://openjdk.java.net/projects/code-tools/jmh/)  micro
benchmark suite to define runners to execute those test cases. You can execute the
default benchmark suite (which takes ~1hour) at once:

```
mvn -Dflink.version=1.5.0 clean install exec:exec
```

There is also a separate benchmark suit for state backend, and you can execute this suit (which takes ~1hour) using
below command:

```
mvn -Dflink.version=1.5.0 clean package exec:exec \
 -Dexec.executable=java -Dexec.args=-jar target/benchmarks.jar -rf csv org.apache.flink.state.benchmark.*
```

If you want to execute just one benchmark, the best approach is to execute selected main function manually.
There're mainly two ways:

1. From your IDE (hint there is a plugin for Intellij IDEA).
   * In this case don't forget about selecting `flink.version`, default value for the property is defined in pom.xml.

2. From command line, using command like:
   ```
   mvn -Dflink.version=1.5.0 clean package exec:exec \
    -Dexec.executable=java -Dexec.args=-jar target/benchmarks.jar <benchmark_class>
   ```

## Code structure

Recommended code structure is to define all benchmarks in [Apache Flink](https://github.com/apache/flink)
and only wrap them here, in this repository, into executor classes. 

Such code structured is due to using GPL2 licensed [jmh](http://openjdk.java.net/projects/code-tools/jmh/) library
for the actual execution of the benchmarks. Ideally we would prefer to have all of the code moved to [Apache Flink](https://github.com/apache/flink)
