"""Complete decision procedure for linear arithmetic predicate implication.

This module implements Fourier-Motzkin elimination for checking unsatisfiability
of linear inequality systems, providing a complete (no unknown results) decision
procedure for linear arithmetic predicates.

Theoretical Foundation:
    Theorem: Linear arithmetic over ℚ (rationals) admits quantifier elimination,
    making predicate implication decidable.

    For p1 ⇒ p2, we check unsatisfiability of p1 ∧ ¬p2:
    - Convert predicates to linear inequalities
    - Apply Fourier-Motzkin elimination to eliminate variables
    - If empty system: satisfiable (p1 ⇏ p2)
    - If contradiction (0 ≤ negative): unsatisfiable (p1 ⇒ p2)

Fourier-Motzkin Elimination:
    Given system S with variable x:
    1. Partition S into:
       - L: inequalities of form a*x ≤ b (lower bounds)
       - U: inequalities of form c*x ≥ d (upper bounds)
       - N: inequalities not involving x
    2. For each pair (l, u) in L × U, generate:
       - Combined inequality eliminating x
    3. New system S' = N ∪ {combined inequalities}
    4. Repeat until no variables remain

Complexity:
    - Fourier-Motzkin can have doubly-exponential blowup in worst case
    - For SQL predicates (typically 2-5 variables), this is tractable
    - We add safeguards for pathological cases

Supported Predicates:
    - x OP c where OP ∈ {<, ≤, =, ≥, >} and c is constant
    - x OP y where OP ∈ {<, ≤, =, ≥, >}
    - a*x + b*y OP c (linear combinations)
    - BETWEEN x AND y (converted to conjunction)
    - Conjunctions of the above

Limitations:
    - Non-linear predicates (x * y, x^2) are not supported
    - String predicates (LIKE) are not supported here
    - NULL handling is simplified (assumes NOT NULL context)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from fractions import Fraction
from typing import Any


class LinearCheckResult(Enum):
    """Result of linear arithmetic check."""

    IMPLIES = "implies"  # p1 ⇒ p2 proven
    NOT_IMPLIES = "not_implies"  # p1 ⇏ p2 proven (counterexample exists)
    NOT_LINEAR = "not_linear"  # Predicates are not linear
    ERROR = "error"  # Error during check


@dataclass
class LinearInequality:
    """A linear inequality of the form: sum(coeffs[var] * var) ≤ constant.

    Represents: a1*x1 + a2*x2 + ... + an*xn ≤ c

    For strict inequalities (<), we use a small epsilon approach or
    convert to non-strict form during processing.

    Attributes:
        coefficients: Map from variable name to coefficient
        constant: Right-hand side constant (negated to put in form ≤ 0)
        is_strict: True for < (strict), False for ≤ (non-strict)
    """

    coefficients: dict[str, Fraction] = field(default_factory=dict)
    constant: Fraction = Fraction(0)
    is_strict: bool = False

    def involves(self, var: str) -> bool:
        """Check if this inequality involves the variable."""
        return var in self.coefficients and self.coefficients[var] != 0

    def get_coefficient(self, var: str) -> Fraction:
        """Get coefficient for a variable (0 if not present)."""
        return self.coefficients.get(var, Fraction(0))

    def is_contradiction(self) -> bool:
        """Check if this is a contradiction (0 ≤ negative)."""
        if all(c == 0 for c in self.coefficients.values()):
            if self.is_strict:
                return self.constant >= 0  # 0 < c means c must be > 0
            else:
                return self.constant > 0  # 0 ≤ c means c must be ≤ 0
        return False

    def is_tautology(self) -> bool:
        """Check if this is a tautology (always true).

        With all zero coefficients, the inequality reduces to:
        - Non-strict: 0 ≤ -constant → true when constant ≤ 0
        - Strict:     0 < -constant → true when constant < 0
        """
        if all(c == 0 for c in self.coefficients.values()):
            if self.is_strict:
                return self.constant < 0
            else:
                return self.constant <= 0
        return False

    def __str__(self) -> str:
        terms = []
        for var, coef in sorted(self.coefficients.items()):
            if coef != 0:
                if coef == 1:
                    terms.append(var)
                elif coef == -1:
                    terms.append(f"-{var}")
                else:
                    terms.append(f"{coef}*{var}")

        lhs = " + ".join(terms) if terms else "0"
        op = "<" if self.is_strict else "≤"
        return f"{lhs} {op} {-self.constant}"


@dataclass
class LinearArithmeticResult:
    """Result of linear arithmetic implication check.

    Attributes:
        result: The check result
        explanation: Human-readable explanation
        system_size: Number of inequalities processed
        variables_eliminated: Number of variables eliminated
    """

    result: LinearCheckResult
    explanation: str
    system_size: int = 0
    variables_eliminated: int = 0


def check_linear_implication(
    p1: Any,
    p2: Any,
    max_inequalities: int = 1000,
    max_variables: int = 20,
) -> LinearArithmeticResult:
    """Check if p1 implies p2 using Fourier-Motzkin elimination.

    This is a complete decision procedure for linear arithmetic:
    - If p1 ⇒ p2, returns IMPLIES
    - If p1 ⇏ p2, returns NOT_IMPLIES
    - If predicates are not linear, returns NOT_LINEAR

    Args:
        p1: Antecedent predicate (must be BoundPredicate or similar)
        p2: Consequent predicate
        max_inequalities: Safety limit on system size
        max_variables: Safety limit on number of variables

    Returns:
        LinearArithmeticResult with the decision
    """
    try:
        # Convert p1 to inequalities
        ineqs_p1, vars_p1 = _predicate_to_inequalities(p1)
        if ineqs_p1 is None:
            return LinearArithmeticResult(
                result=LinearCheckResult.NOT_LINEAR,
                explanation="p1 is not a linear arithmetic predicate",
            )

        # Convert ¬p2 to inequalities
        ineqs_not_p2, vars_p2 = _predicate_to_inequalities(p2, negate=True)
        if ineqs_not_p2 is None:
            return LinearArithmeticResult(
                result=LinearCheckResult.NOT_LINEAR,
                explanation="p2 is not a linear arithmetic predicate",
            )

        # Combine: p1 ∧ ¬p2
        system = ineqs_p1 + ineqs_not_p2
        all_vars = vars_p1 | vars_p2

        # Safety checks
        if len(system) > max_inequalities:
            return LinearArithmeticResult(
                result=LinearCheckResult.NOT_LINEAR,
                explanation=f"System too large ({len(system)} inequalities)",
                system_size=len(system),
            )

        if len(all_vars) > max_variables:
            return LinearArithmeticResult(
                result=LinearCheckResult.NOT_LINEAR,
                explanation=f"Too many variables ({len(all_vars)})",
                system_size=len(system),
            )

        # Apply Fourier-Motzkin elimination
        is_unsat, _final_system, vars_eliminated = _fourier_motzkin(
            system, all_vars, max_inequalities
        )

        if is_unsat:
            return LinearArithmeticResult(
                result=LinearCheckResult.IMPLIES,
                explanation="p1 ∧ ¬p2 is unsatisfiable (Fourier-Motzkin)",
                system_size=len(system),
                variables_eliminated=vars_eliminated,
            )
        else:
            return LinearArithmeticResult(
                result=LinearCheckResult.NOT_IMPLIES,
                explanation="p1 ∧ ¬p2 is satisfiable (counterexample exists)",
                system_size=len(system),
                variables_eliminated=vars_eliminated,
            )

    except Exception as e:
        return LinearArithmeticResult(
            result=LinearCheckResult.ERROR,
            explanation=f"Error during linear arithmetic check: {e}",
        )


def _fourier_motzkin(
    system: list[LinearInequality],
    variables: set[str],
    max_size: int,
) -> tuple[bool, list[LinearInequality], int]:
    """Apply Fourier-Motzkin elimination to check satisfiability.

    Returns:
        (is_unsatisfiable, final_system, variables_eliminated)
    """
    current_system = list(system)
    vars_eliminated = 0

    for var in sorted(variables):  # Sort for determinism
        # Check for contradictions before elimination
        for ineq in current_system:
            if ineq.is_contradiction():
                return True, current_system, vars_eliminated

        # Remove tautologies
        current_system = [ineq for ineq in current_system if not ineq.is_tautology()]

        # Eliminate variable
        current_system = _eliminate_variable(current_system, var)
        vars_eliminated += 1

        # Safety check
        if len(current_system) > max_size:
            # System blew up - fall back to unknown
            # This shouldn't happen for typical SQL predicates
            raise ValueError(f"System size exceeded {max_size} after eliminating {var}")

    # All variables eliminated - check for contradictions
    for ineq in current_system:
        if ineq.is_contradiction():
            return True, current_system, vars_eliminated

    return False, current_system, vars_eliminated


def _eliminate_variable(
    system: list[LinearInequality],
    var: str,
) -> list[LinearInequality]:
    """Eliminate a variable from the system using Fourier-Motzkin.

    Partitions inequalities into:
    - lower: a*x ≤ b (coefficient > 0)
    - upper: -c*x ≤ -d, i.e., c*x ≥ d (coefficient < 0)
    - neutral: no x

    Then combines each lower with each upper to eliminate x.
    """
    lower: list[LinearInequality] = []  # x ≤ ...
    upper: list[LinearInequality] = []  # x ≥ ...
    neutral: list[LinearInequality] = []

    for ineq in system:
        coef = ineq.get_coefficient(var)
        if coef > 0:
            lower.append(ineq)
        elif coef < 0:
            upper.append(ineq)
        else:
            neutral.append(ineq)

    # Generate new inequalities by combining lower and upper bounds
    new_system = list(neutral)

    for lo in lower:
        for up in upper:
            combined = _combine_inequalities(lo, up, var)
            if combined:
                new_system.append(combined)

    return new_system


def _combine_inequalities(
    lower: LinearInequality,
    upper: LinearInequality,
    var: str,
) -> LinearInequality | None:
    """Combine two inequalities to eliminate a variable.

    Given:
    - lower: a*x + ... ≤ c  (a > 0)
    - upper: -b*x + ... ≤ d  (b > 0, so original was b*x ≥ -d)

    Multiply lower by b, upper by a, and add to eliminate x:
    b*(a*x + ...) + a*(-b*x + ...) ≤ b*c + a*d

    The x terms cancel: a*b*x - a*b*x = 0
    """
    a = lower.get_coefficient(var)  # positive
    b = -upper.get_coefficient(var)  # also positive (negated)

    if a <= 0 or b <= 0:
        return None  # Should not happen if called correctly

    # New coefficients: b * lower.coeffs + a * upper.coeffs
    new_coeffs: dict[str, Fraction] = {}

    all_vars = set(lower.coefficients.keys()) | set(upper.coefficients.keys())
    for v in all_vars:
        if v == var:
            continue  # This variable is being eliminated
        new_coef = b * lower.get_coefficient(v) + a * upper.get_coefficient(v)
        if new_coef != 0:
            new_coeffs[v] = new_coef

    # New constant: b * lower.constant + a * upper.constant
    new_constant = b * lower.constant + a * upper.constant

    # Strictness: if either is strict, result is strict
    new_strict = lower.is_strict or upper.is_strict

    return LinearInequality(
        coefficients=new_coeffs,
        constant=new_constant,
        is_strict=new_strict,
    )


def _predicate_to_inequalities(
    pred: Any,
    negate: bool = False,
) -> tuple[list[LinearInequality] | None, set[str]]:
    """Convert a predicate to a list of linear inequalities.

    Returns (inequalities, variables) or (None, empty) if not linear.
    """
    variables: set[str] = set()

    # Duck type: logical predicate (AND/OR)
    if hasattr(pred, "operator") and hasattr(pred, "operands"):
        op = str(pred.operator).upper()

        if "AND" in op:
            # Conjunction: collect all inequalities
            all_ineqs: list[LinearInequality] = []
            for operand in pred.operands:
                sub_ineqs, sub_vars = _predicate_to_inequalities(operand, negate)
                if sub_ineqs is None:
                    return None, set()
                all_ineqs.extend(sub_ineqs)
                variables.update(sub_vars)
            return all_ineqs, variables

        elif "OR" in op:
            if negate:
                # ¬(A ∨ B) = ¬A ∧ ¬B
                all_ineqs = []
                for operand in pred.operands:
                    sub_ineqs, sub_vars = _predicate_to_inequalities(operand, negate=True)
                    if sub_ineqs is None:
                        return None, set()
                    all_ineqs.extend(sub_ineqs)
                    variables.update(sub_vars)
                return all_ineqs, variables
            else:
                # OR is harder - would need to track disjunctions
                # For now, return not linear
                return None, set()

        elif "NOT" in op:
            # NOT(p) = negate the inner
            if pred.operands:
                return _predicate_to_inequalities(pred.operands[0], negate=not negate)

    # Duck type: comparison predicate
    if hasattr(pred, "left") and hasattr(pred, "operator") and hasattr(pred, "right"):
        return _comparison_to_inequalities(pred, negate)

    # Duck type: BETWEEN predicate
    if hasattr(pred, "expression") and hasattr(pred, "low") and hasattr(pred, "high"):
        return _between_to_inequalities(pred, negate)

    # Duck type: IS NULL - not linear
    if hasattr(pred, "expression") and hasattr(pred, "negated") and not hasattr(pred, "values"):
        return None, set()

    # Duck type: IN predicate - could convert to OR, but complex
    if hasattr(pred, "expression") and hasattr(pred, "values"):
        return None, set()

    return None, set()


def _comparison_to_inequalities(
    pred: Any,
    negate: bool,
) -> tuple[list[LinearInequality] | None, set[str]]:
    """Convert a comparison predicate to inequalities.

    Supported forms:
    - column OP constant
    - column OP column
    - constant OP column
    """
    left = pred.left
    right = pred.right
    op = str(pred.operator)

    # Extract left side
    left_coefs, left_const, left_vars = _expr_to_linear(left)
    if left_coefs is None:
        return None, set()

    # Extract right side
    right_coefs, right_const, right_vars = _expr_to_linear(right)
    if right_coefs is None:
        return None, set()

    all_vars = left_vars | right_vars

    # Move everything to left: left - right OP 0
    combined_coefs: dict[str, Fraction] = dict(left_coefs)
    for var, coef in right_coefs.items():
        combined_coefs[var] = combined_coefs.get(var, Fraction(0)) - coef
    combined_const = right_const - left_const  # Note: moved to RHS

    # Handle negation
    if negate:
        op = _negate_op(op)

    # Convert to standard form: ... ≤ constant
    return _op_to_inequalities(combined_coefs, combined_const, op, all_vars)


def _between_to_inequalities(
    pred: Any,
    negate: bool,
) -> tuple[list[LinearInequality] | None, set[str]]:
    """Convert BETWEEN predicate to inequalities.

    x BETWEEN a AND b  →  x ≥ a AND x ≤ b
    NOT (x BETWEEN a AND b)  →  x < a OR x > b  (not linear in OR form)
    """
    if negate:
        # NOT BETWEEN creates OR - not linear
        return None, set()

    expr_coefs, expr_const, expr_vars = _expr_to_linear(pred.expression)
    if expr_coefs is None:
        return None, set()

    low_coefs, low_const, low_vars = _expr_to_linear(pred.low)
    if low_coefs is None:
        return None, set()

    high_coefs, high_const, high_vars = _expr_to_linear(pred.high)
    if high_coefs is None:
        return None, set()

    all_vars = expr_vars | low_vars | high_vars

    # x ≥ low  →  -x + low ≤ 0  →  low - x ≤ 0
    lower_coefs: dict[str, Fraction] = {}
    for var, coef in low_coefs.items():
        lower_coefs[var] = coef
    for var, coef in expr_coefs.items():
        lower_coefs[var] = lower_coefs.get(var, Fraction(0)) - coef
    lower_const = expr_const - low_const

    # x ≤ high  →  x - high ≤ 0
    upper_coefs: dict[str, Fraction] = {}
    for var, coef in expr_coefs.items():
        upper_coefs[var] = coef
    for var, coef in high_coefs.items():
        upper_coefs[var] = upper_coefs.get(var, Fraction(0)) - coef
    upper_const = high_const - expr_const

    return [
        LinearInequality(coefficients=lower_coefs, constant=lower_const),
        LinearInequality(coefficients=upper_coefs, constant=upper_const),
    ], all_vars


def _expr_to_linear(
    expr: Any,
) -> tuple[dict[str, Fraction] | None, Fraction, set[str]]:
    """Convert an expression to linear form: coefficients and constant.

    Returns (coefficients, constant, variables) or (None, 0, {}) if not linear.
    """
    # Duck type: column reference
    if hasattr(expr, "qualified_id") and callable(getattr(expr, "qualified_id", None)):
        var_name = expr.qualified_id()
        return {var_name: Fraction(1)}, Fraction(0), {var_name}

    # Duck type: literal
    if hasattr(expr, "value"):
        try:
            val = Fraction(expr.value)
            return {}, val, set()
        except (ValueError, TypeError):
            return None, Fraction(0), set()

    # Duck type: binary expression (a + b, a - b, a * const)
    if hasattr(expr, "operator") and hasattr(expr, "left") and hasattr(expr, "right"):
        left_coefs, left_const, left_vars = _expr_to_linear(expr.left)
        right_coefs, right_const, right_vars = _expr_to_linear(expr.right)

        if left_coefs is None or right_coefs is None:
            return None, Fraction(0), set()

        op = str(expr.operator)

        if op in ("+", "ADD"):
            # Add coefficients
            combined: dict[str, Fraction] = dict(left_coefs)
            for var, coef in right_coefs.items():
                combined[var] = combined.get(var, Fraction(0)) + coef
            return combined, left_const + right_const, left_vars | right_vars

        elif op in ("-", "SUB", "SUBTRACT"):
            # Subtract coefficients
            combined = dict(left_coefs)
            for var, coef in right_coefs.items():
                combined[var] = combined.get(var, Fraction(0)) - coef
            return combined, left_const - right_const, left_vars | right_vars

        elif op in ("*", "MUL", "MULTIPLY"):
            # Multiplication - only linear if one side is constant
            if not left_coefs and not right_coefs:
                # Both constants
                return {}, left_const * right_const, set()
            elif not left_coefs:
                # Left is constant, multiply right by it
                scaled: dict[str, Fraction] = {}
                for var, coef in right_coefs.items():
                    scaled[var] = coef * left_const
                return scaled, right_const * left_const, right_vars
            elif not right_coefs:
                # Right is constant, multiply left by it
                scaled = {}
                for var, coef in left_coefs.items():
                    scaled[var] = coef * right_const
                return scaled, left_const * right_const, left_vars
            else:
                # Both have variables - not linear
                return None, Fraction(0), set()

    # Unknown expression type
    return None, Fraction(0), set()


def _negate_op(op: str) -> str:
    """Negate a comparison operator."""
    negations = {
        "=": "!=",
        "==": "!=",
        "!=": "=",
        "<>": "=",
        "<": ">=",
        "<=": ">",
        ">": "<=",
        ">=": "<",
        "EQ": "NE",
        "NE": "EQ",
        "LT": "GE",
        "LE": "GT",
        "GT": "LE",
        "GE": "LT",
    }
    return negations.get(op, op)


def _op_to_inequalities(
    coefs: dict[str, Fraction],
    const: Fraction,
    op: str,
    variables: set[str],
) -> tuple[list[LinearInequality] | None, set[str]]:
    """Convert an operator to inequalities.

    Form: sum(coefs[v] * v) OP const
    """
    op_upper = op.upper()

    if op_upper in ("=", "==", "EQ"):
        # Equality: need two inequalities (≤ and ≥)
        return [
            LinearInequality(coefficients=dict(coefs), constant=-const),  # ≤
            LinearInequality(
                coefficients={v: -c for v, c in coefs.items()},
                constant=const,
            ),  # ≥ (negated to ≤)
        ], variables

    elif op_upper in ("!=", "<>", "NE"):
        # Inequality - can't express as linear inequalities
        # Would need disjunction: < OR >
        return None, set()

    elif op_upper in ("<", "LT"):
        # Strict less than
        return [
            LinearInequality(coefficients=dict(coefs), constant=-const, is_strict=True)
        ], variables

    elif op_upper in ("<=", "LE"):
        # Less than or equal
        return [LinearInequality(coefficients=dict(coefs), constant=-const)], variables

    elif op_upper in (">", "GT"):
        # Strict greater than: negate to -(...) < -const
        return [
            LinearInequality(
                coefficients={v: -c for v, c in coefs.items()},
                constant=const,
                is_strict=True,
            )
        ], variables

    elif op_upper in (">=", "GE"):
        # Greater than or equal: negate
        return [
            LinearInequality(
                coefficients={v: -c for v, c in coefs.items()},
                constant=const,
            )
        ], variables

    return None, set()


def is_linear_predicate(pred: Any) -> bool:
    """Check if a predicate is linear (can be converted to linear inequalities).

    This is a fast check that doesn't do full conversion.
    """
    # Duck type: logical AND
    if hasattr(pred, "operator") and hasattr(pred, "operands"):
        op = str(pred.operator).upper()
        if "AND" in op:
            return all(is_linear_predicate(op) for op in pred.operands)
        elif "OR" in op:
            return False  # OR creates disjunction
        elif "NOT" in op:
            # NOT is only linear if inner is simple comparison
            if pred.operands:
                inner = pred.operands[0]
                return hasattr(inner, "left") and hasattr(inner, "operator")

    # Duck type: comparison
    if hasattr(pred, "left") and hasattr(pred, "operator") and hasattr(pred, "right"):
        op = str(pred.operator).upper()
        # Inequality (!=) is not linear
        if op in ("!=", "<>", "NE"):
            return False
        return _is_linear_expr(pred.left) and _is_linear_expr(pred.right)

    # Duck type: BETWEEN
    if hasattr(pred, "expression") and hasattr(pred, "low") and hasattr(pred, "high"):
        return (
            _is_linear_expr(pred.expression)
            and _is_linear_expr(pred.low)
            and _is_linear_expr(pred.high)
        )

    return False


def _is_linear_expr(expr: Any) -> bool:
    """Check if an expression is linear."""
    # Column reference - linear
    if hasattr(expr, "qualified_id"):
        return True

    # Literal - linear (constant)
    if hasattr(expr, "value"):
        try:
            float(expr.value)
            return True
        except (ValueError, TypeError):
            return False

    # Binary expression
    if hasattr(expr, "operator") and hasattr(expr, "left") and hasattr(expr, "right"):
        op = str(expr.operator)
        if op in ("+", "-", "ADD", "SUB", "SUBTRACT"):
            return _is_linear_expr(expr.left) and _is_linear_expr(expr.right)
        elif op in ("*", "MUL", "MULTIPLY"):
            # Linear only if one side is constant
            left_const = hasattr(expr.left, "value")
            right_const = hasattr(expr.right, "value")
            if left_const:
                return _is_linear_expr(expr.right)
            elif right_const:
                return _is_linear_expr(expr.left)
            return False

    return False
