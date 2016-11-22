# Reactive Async

## Building

Building Reactive Async requires
[sbt](http://www.scala-sbt.org). Follow these steps:

```
$ sbt
> project lib
> compile
```

To package the Reactive Async library into a `jar` file use `package`
instead of `compile`.

## Testing

The test suite is based on [ScalaTest](http://www.scalatest.org); it
can be run from the sbt prompt:

```
$ sbt
> project lib
> test
```

Note that currently this will also run benchmarks, which consumes a
fair amount of memory. Thus, it might be necessary to increase the
JVM's maximum heap size before starting sbt.
