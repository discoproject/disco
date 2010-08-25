"""
:mod:`discodb.query` -- Supporting objects for the DiscoDB query interface
==========================================================================

>>> Q.parse('~(B | C)') == Q.parse('~B & ~C')
True
>>> Q.parse('(A & B) | C') == Q.parse('(A | C) & (B | C)')
True
>>> Q.parse('A & (B | (D & E))') == Q.parse('A & (B | D) & (B | E)')
True
>>> Q.parse(str(Q.parse('a | b | c & d | e'))) == Q.parse('a | b | c & d | e')
True
>>> Q.urlscan(Q.parse('(a | b) & ~c').urlformat()) == Q.parse('~c & (a | b)')
True

>>> from .discodb import DiscoDB
>>> discodb = DiscoDB({'A': ['B', 'C'], 'B': 'D', 'C': 'E', 'D': 'F', 'E': 'G'})
>>> sorted(discodb.query(Q.parse('A')))
['B', 'C']
>>> sorted(discodb.query(Q.parse('*A')))
['D', 'E']
>>> sorted(discodb.query(Q.parse('A | B')))
['B', 'C', 'D']
>>> sorted(discodb.query(Q.parse('*A | B')))
['D', 'E']
>>> sorted(discodb.query(Q.parse('**A | *B')))
['F', 'G']

>>> sorted((str(k), sorted(vs)) for k, vs in discodb.metaquery(Q.parse('A')))
[('(A)', ['B', 'C'])]
>>> sorted((str(k), sorted(vs)) for k, vs in discodb.metaquery(Q.parse('*A')))
[('(B)', ['D']), ('(C)', ['E'])]
>>> sorted((str(k), sorted(vs)) for k, vs in discodb.metaquery(Q.parse('A | B')))
[('(A | B)', ['B', 'C', 'D'])]
>>> sorted((str(k), sorted(vs)) for k, vs in discodb.metaquery(Q.parse('*A | B')))
[('(B)', ['D']), ('(C | B)', ['D', 'E'])]
>>> sorted((str(k), sorted(vs)) for k, vs in discodb.metaquery(Q.parse('**A | *B')))
[('(D)', ['F']), ('(E | D)', ['F', 'G'])]
"""
from operator import __and__, __or__

class Q(object):
    """
    Contains a conjunction of clauses.
    """
    def __init__(self, clauses):
        self.clauses = frozenset(clauses)

    def __and__(self, other):
        """
        Conjunction of Qs is always a Q.
        """
        return Q(self.clauses | other.clauses)

    def __or__(self, other):
        """
        Disjunction of Qs is always a Q.
        """
        return Q(c | d for c in self.clauses
                 for d in other.clauses)

    def __invert__(self):
        """
        Inversion of a Q is always a Q.

        ~(c & ...) -> (~c) | ... -> ~(C) -> Q
        """
        if not self.clauses:
            return self
        return Q.wrap(reduce(__or__, (~c for c in self.clauses)))

    def __pos__(self):
        return Q((Clause((MetaLiteral(self), )), ))

    def __cmp__(self, other):
        return cmp(str(self), str(other))

    def __eq__(self, other):
        return self.clauses == other.clauses

    def __hash__(self):
        return hash(self.clauses)

    def __str__(self):
        return ' & '.join('(%s)' % c for c in self.clauses)

    def expand(self, discodb):
        from itertools import product
        for combo in product(*(c.expand(discodb) for c in self.clauses)):
            yield reduce(__and__, combo)

    def metaquery(self, discodb):
        for q in self.expand(discodb):
            yield q, discodb.query(q)

    def resolve(self, discodb):
        return reduce(__and__, (c.resolve(discodb) for c in self.clauses), Q([]))

    def urlformat(self, safe=':()/,~'):
        from urllib import quote
        return quote(str(self)
                     .replace('&', '/')
                     .replace('|', ',')
                     .replace(' ' ,''),
                     safe)

    @classmethod
    def urlscan(cls, string):
        from urllib import unquote
        return cls.parse(unquote(string)\
                         .strip().strip('/')\
                         .replace('/', '&')\
                         .replace(',', '|'))

    @classmethod
    def parse(cls, string):
        """
        Parse a string into a Q object.

        The parsed string is subject to the following conditions:

         - `*` characters will be replaced with `+`
         - literal terms cannot contain the characters `&|~+()`
         - leading and trailing spaces will be stripped from terms
         - the characters `&|~` signify the corresponding logical operations
         - parentheses make operator precedence explicit
         - `+` characters introduce a dereferencing operation

        The string will be parsed into a logical combination of the terms,
        which will be stored internally in :term:`conjunctive normal form`.

        Dereferencing is performed when the
        :class:`Q` object is actually used to query a :class:`discodb.DiscoDB`.

        In queries (:meth:`discodb.DiscoDB.query`),
        the dereferencing operation replaces an expression with
        the union of the values which result from querying with the expression.

        Thus in the case of::

                discodb = DiscoDB({'A': ['B', 'C'], 'B': 'D'})

        The following queries would be equivalent::

                discodb.query(Q.parse('+A'))
                discodb.query(Q.parse('*A'))
                discodb.query(Q.parse('B | C'))

        and would result in an iterator containing only the value ``'D'``.

        In metaqueries (:meth:`discodb.DiscoDB.metaquery`),
        the dereferencing operation replaces an expression with
        each value which results from querying with the expression,
        and yields a (query, values) pair for each item in the expansion.

        Thus, for the discodb above::

                discodb.metaquery('A')

        would return an iterator roughly equivalent to
        `[(Q.parse('A'), ['B', 'C'])]`, while::

                discodb.metaquery('*A')

        would return an iterator roughly equivalent to
        `[(Q.parse('B'), ['D']), (Q.parse('C'), [])]`.
        """
        import re
        return eval(re.sub(r'([^&|~+()\s][^&|~+()]*)',
                           r'Q.wrap("""\1""".strip())',
                           string.replace('*', '+')) or 'Q([])')

    @classmethod
    def wrap(cls, proposition):
        """
        Consider any object as a proposition and return a well-behaved Q.
        """
        if isinstance(proposition, Q):
            return proposition
        if isinstance(proposition, Clause):
            return Q((proposition, ))
        if isinstance(proposition, Literal):
            return Q((Clause((proposition, )), ))
        return Q((Clause((Literal(proposition), )), ))

class Clause(object):
    """
    Contains a disjunction of literals.
    """
    def __init__(self, literals):
        self.literals = frozenset(literals)

    def __and__(self, other):
        """
        Conjunction of Clauses is always a Q.
        """
        return Q((self, other))

    def __or__(self, other):
        """
        Disjunction of Clauses is always a Clause.
        """
        return Clause(self.literals | other.literals)

    def __invert__(self):
        """
        Inversion of a Clause is always a Q.

        ~(l | ...) -> (~l) & ... -> Q
        """
        return Q.wrap(reduce(__and__, (~l for l in self.literals)))

    def __eq__(self, other):
        return self.literals == other.literals

    def __hash__(self):
        return hash(self.literals)

    def __str__(self):
        return ' | '.join('%s' % l for l in self.literals)

    def expand(self, discodb):
        from itertools import product
        for combo in product(*(l.expand(discodb) for l in self.literals)):
            yield reduce(__or__, combo)

    def resolve(self, discodb):
        return reduce(__or__, (l.resolve(discodb) for l in self.literals))

class Literal(object):
    """
    A potential key in a discodb (or its negation).
    """
    def __init__(self, term, negated=False):
        self.term    = term
        self.negated = negated

    def __and__(self, other):
        """
        Conjunction of Literals is always a Q.
        """
        return Clause((self, )) & Clause((other, ))

    def __or__(self, other):
        """
        Disjunction of Literals is always a Clause.
        """
        return Clause((self, other))

    def __invert__(self):
        """
        Inversion of a Literal is always a Literal.
        """
        return type(self)(self.term, negated=not self.negated)

    def __eq__(self, other):
        return self.term == other.term and self.negated == other.negated

    def __hash__(self):
        return hash(self.term) ^ hash(self.negated)

    def __str__(self):
        return '%s%s' % ('~' if self.negated else '', self.term)

    def expand(self, discodb):
        yield Q.wrap(self)

    def resolve(self, discodb):
        return Q.wrap(self)

class MetaLiteral(Literal):
    def __str__(self):
        return '%s+(%s)' % ('~' if self.negated else '', self.term)

    def expand(self, discodb):
        for q in self.term.expand(discodb):
            for v in discodb.query(q):
                yield not Q.wrap(v) if self.negated else Q.wrap(v)

    def resolve(self, discodb):
        q = self.term.resolve(discodb)
        clause = Clause(Literal(v) for v in discodb.query(q))
        return Q.wrap(not clause if self.negated else clause)
