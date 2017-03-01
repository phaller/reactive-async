# Reactive Async

## Building

Building Reactive Async requires
[sbt](http://www.scala-sbt.org). Follow these steps:

```
$ sbt
> project core
> compile
```

To package the Reactive Async library into a `jar` file use `package`
instead of `compile`.

## Testing

The test suite (based on [ScalaTest](http://www.scalatest.org)) is run
as follows:

```
$ sbt
> project core
> test
```

## Benchmarking

### Microbenchmarks

The microbenchmarks (based on
[ScalaMeter](https://scalameter.github.io)) are run as follows:

```
$ sbt
> project bench
> test
```

Note that this consumes a fair amount of memory. Thus, it might be
necessary to increase the JVM's maximum heap size before starting sbt.
