package com.phaller.rasync

/**
 * Use this trait in callbacks to return the new value of a cell.
 * `NextOutcome(v)` and `FinalOutcome(v)` put the value `v` into
 * the cell; in the latter case the cell is completed.
 * Use `NoOutcome` to indicate that no progress is possible.
 */
sealed trait Outcome[+V]
sealed trait ValueOutcome[+V] extends Outcome[V] {
  val value: V
}
final case class NextOutcome[+V](override val value: V) extends ValueOutcome[V]
final case class FinalOutcome[+V](override val value: V) extends ValueOutcome[V]
case object NoOutcome extends Outcome[Nothing]

object Outcome {

  /** Returns a `NextOutcome(value)` or `FinalOutcome(value)` object, depending on `isFinal`. */
  def apply[V](value: V, isFinal: Boolean): ValueOutcome[V] =
    if (isFinal) FinalOutcome(value)
    else NextOutcome(value)

  /**
   * Returns a `NextOutcome`, `FinalOutcome`, or `NoOutcome` object.
   *
   * If `valueOpt` is `None`, `NoOutcome` is returned. Otherwise, a `NextOutcome` or
   * `FinalOutcome` is returned depending on `isFinal`.
   *
   * @param valueOpt  Option of a new value.
   * @param isFinal   Indicates if the value is final.
   * @return          Returns a `NextOutcome`, `FinalOutcome`, or `NoOutcome` object.
   */
  def apply[V](valueOpt: Option[V], isFinal: Boolean): Outcome[V] =
    valueOpt.map(value => if (isFinal) FinalOutcome(value) else NextOutcome(value)).getOrElse(NoOutcome)

  /**
   * Returns a `NextOutcome`, `FinalOutcome`, or `NoOutcome` object.
   *
   * If `valueOpt` is `None`, `NoOutcome` is returned. Otherwise, a `NextOutcome` or
   * `FinalOutcome` is returned depending on the boolean parameter.
   *
   * @param valueOpt  Option of a new value, a pair of a value V and a boolean to indicate if it is final.
   * @return          Returns a `NextOutcome`, `FinalOutcome`, or `NoOutcome` object.
   */
  def apply[V](valueOpt: Option[(V, Boolean)]): Outcome[V] = valueOpt match {
    case Some((v, false)) => NextOutcome(v)
    case Some((v, true)) => FinalOutcome(v)
    case None => NoOutcome
  }

  /** Match outcomes. */
  def unapply[V](outcome: Outcome[V]): Option[(V, Boolean)] = outcome match {
    case FinalOutcome(v) => Some(v, true)
    case NextOutcome(v) => Some(v, false)
    case _ => None
  }

  /** Match non-empty outcomes. */
  def unapply[V](outcome: ValueOutcome[V]): (V, Boolean) = outcome match {
    case FinalOutcome(v) => (v, true)
    case NextOutcome(v) => (v, false)
  }
}
