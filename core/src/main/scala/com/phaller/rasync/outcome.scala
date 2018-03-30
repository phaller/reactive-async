package com.phaller.rasync

/**
 * Use this trait in callbacks to return the new value of a cell.
 * `NextOutcome(v)` and `FinalOutcome(v)` put the value `v` into
 * the cell; in the latter case the cell is completed.
 * Use `NoOutcome` to indicate that no progress is possible.
 */
sealed trait Outcome[+V]
final case class NextOutcome[+V](value: V) extends Outcome[V]
final case class FinalOutcome[+V](value: V) extends Outcome[V]
case object NoOutcome extends Outcome[Nothing]

object Outcome {

  /** Returns a `NextOutcome(value)` or `FinalOutcome(value)` object, depending on `isFinal`. */
  def apply[V](value: V, isFinal: Boolean): Outcome[V] =
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

  /** Match non-empty outcomes. */
  def unapply[V](outcome: Outcome[V]): Option[(V, Boolean)] = outcome match {
    case FinalOutcome(v) => Some(v, true)
    case NextOutcome(v) => Some(v, false)
    case _ => None
  }
}
