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

Talks:

- Talk at Scala Days 2016: [video](https://www.youtube.com/watch?v=S9xxhyDYoZk),
  [slides](https://speakerdeck.com/phaller/programming-with-futures-lattices-and-quiescence)

- Talk at ACM SIGPLAN Scala Symposium 2016:
  [slides](https://speakerdeck.com/phaller/reactive-async-expressive-deterministic-concurrency)

- Talk at ISSTA 2020:
  [video](https://www.youtube.com/watch?v=ejueBIa6FBY&t=1313)

Papers and thesis:

- Dominik Helm, Florian K&uuml;bler, Jan Thomas K&ouml;lzer, Philipp Haller, Michael Eichberg, Guido Salvaneschi and Mira Mezini.
  [A programming model for semi-implicit parallelization of static analyses](http://www.csc.kth.se/~phaller/doc/helm20-issta.pdf).
  Proc. ACM SIGSOFT International Symposium on Software Testing and Analysis. ACM, 2020. [[ACM DL](https://dl.acm.org/doi/10.1145/3395363.3397367)]

- Philipp Haller, Simon Geries, Michael Eichberg, and Guido Salvaneschi.
  [Reactive Async: Expressive Deterministic Concurrency](http://www.csc.kth.se/~phaller/doc/haller16-scala.pdf).
  Proc. ACM SIGPLAN Scala Symposium. ACM, 2016. [[ACM DL](http://dl.acm.org/citation.cfm?id=2998396)]

- Master's thesis: Simon Geries. [Reactive Async: Safety and efficiency
  of new abstractions for reactive, asynchronous
  programming](http://urn.kb.se/resolve?urn=urn%3Anbn%3Ase%3Akth%3Adiva-191330). KTH,
  School of Computer Science and Communication (CSC). 2016.

## Contributing

Reactive Async is published under the [BSD 2-Clause
License](https://opensource.org/licenses/BSD-2-Clause) (see file
`LICENSE` in the project's root directory). Contributions submitted
using the normal means to contribute to the project--such as pull
requests and patches--indicate the contributors' assent for inclusion
of that software in the canonical version under the project's license.

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
