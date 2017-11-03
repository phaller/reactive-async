package immutability;

// The following mutabilities are defined w.r.t. the case that the class hierarchy is closed.

abstract class Root( final val l: Int) // O: Immutable // T: Mutable

class SRootI extends Root(1) // O: Immutable // T: Conditionally Immutable

class SRootM(var j: Int) extends Root(1) // O: Mutable // T: Mutable

class SSRootMN extends SRootM(-1) // O: Mutable // T: Mutable

class SSRootICM( final val o: SRootM) extends SRootI // O: Conditionally Immutable // T: Conditionally Immutable

class SRootI_EF extends Root(1) { // [If we have an analysis for effectively final fields…] // O: Immutable // T: Immutable
  // otherwise: // // O: Mutable // T: Mutable

  // The scala compiler does not generate a setter for the following private field.
  private[this] var x: Int = 100;

  override def toString: String = "SRootI with effectively final field"
}

class SSRootI_EF_I extends SRootI_EF // O: Immutable // T: Immutable

final class SRootII extends Root(1) // O: Immutable // T: Immutable

class X(val i: Int) // O: Immutable // T: Mutable

class Y(var j: Int) extends X(j) // O: Mutable // T: Mutable

class U { // O: ConditionallyImmutable // T: ConditionallyImmutable
  val x: X = new X(10)
}

class V {
  val u: U = new U
}

/*
RESULTS WHEN USING APPLICATION MODE (CLOSED CLASS HIERARCHY)
[info]                immutability.Root => ImmutableObject => MutableType
[info]                immutability.SRootI => ImmutableObject => ConditionallyImmutableType
[info]                immutability.SRootII => ImmutableObject => ImmutableType
[info]                immutability.SSRootICM => ConditionallyImmutableObject => ConditionallyImmutableType
[info]                immutability.SRootI_EF => MutableObjectByAnalysis => MutableType
[info]                immutability.SRootM => MutableObjectByAnalysis => MutableType
[info]                immutability.SSRootI_EF_I => MutableObjectByAnalysis => MutableType
[info]                immutability.SSRootMN => MutableObjectByAnalysis => MutableType

RESULTS WHEN USING LIBRAY WITH CLOSED PACKAGES ASSUMPTION
[info]                immutability.Root => ImmutableObject => MutableType
[info]                immutability.SRootI => ImmutableObject => MutableType (*)
[info]                immutability.SRootII => ImmutableObject => ImmutableType
[info]                immutability.SSRootICM => ConditionallyImmutableObject => MutableType(*)
[info]                immutability.SRootI_EF => MutableObjectByAnalysis => MutableType
[info]                immutability.SRootM => MutableObjectByAnalysis => MutableType
[info]                immutability.SSRootI_EF_I => MutableObjectByAnalysis => MutableType
[info]                immutability.SSRootMN => MutableObjectByAnalysis => MutableType
*/
