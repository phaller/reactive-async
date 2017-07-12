# Reactive Async

Reactive Async is a concurrent programming model, which decouples
concurrent computations using so-called *cells*, shared locations
which generalize
[futures](https://en.wikipedia.org/wiki/Futures_and_promises) as well
as deterministic abstractions such as
[LVars](https://hackage.haskell.org/package/lvish). Compared to
previously proposed programming models Reactive Async provides (a) a
fallback mechanism for the case where no computation ever computes the
value of a given cell, and (b) explicit and optimized handling of
*cyclic dependencies* between cells. In this repository you find a
complete implementation of the Reactive Async programming model in and
for Scala.

Talk at Scala Days 2016: [video](https://www.youtube.com/watch?v=S9xxhyDYoZk),
[slides](https://speakerdeck.com/phaller/programming-with-futures-lattices-and-quiescence)

Talk at ACM SIGPLAN Scala Symposium 2016:
[slides](https://speakerdeck.com/phaller/reactive-async-expressive-deterministic-concurrency)

Paper: Philipp Haller, Simon Geries, Michael Eichberg, and Guido Salvaneschi.
       [Reactive Async: Expressive Deterministic Concurrency](http://www.csc.kth.se/~phaller/doc/haller16-scala.pdf).
       Proc. ACM SIGPLAN Scala Symposium. ACM, 2016. [[ACM DL](http://dl.acm.org/citation.cfm?id=2998396)]

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
