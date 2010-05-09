"""
Supporting Python objects for the query interface.

>>> Q.parse('~(B | C)') == Q.parse('~B & ~C')
True
>>> Q.parse('(A & B) | C') == Q.parse('(A | C) & (B | C)')
True
>>> Q.parse('A & (B | (D & E))') == Q.parse('A & (B | D) & (B | E)')
True
>>> Q.scan(Q.parse('a | b | c & d | e').format()) == Q.parse('a | b | c & d | e')
True
>>> Q.urlscan(Q.parse('(a | b) & ~c').urlformat()) == Q.parse('~c & (a | b)')
True
"""

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
        from operator import __or__
        return Q.wrap(reduce(__or__, (~c for c in self.clauses)))

    def __eq__(self, other):
        return self.clauses == other.clauses

    def __hash__(self):
        return hash(self.clauses)

    def __str__(self):
        return ' & '.join('(%s)' % c for c in self.clauses)

    def format(self, and_op='&', or_op='|', not_op='~', encoding=str):
        return and_op.join(c.format(or_op=or_op,
                                    not_op=not_op,
                                    encoding=encoding) for c in self.clauses)

    @classmethod
    def scan(cls, string, and_op='&', or_op='|', not_op='~', decoding=str):
        return Q(Clause.scan(c, or_op=or_op,
                             not_op=not_op,
                             decoding=decoding) for c in string.split(and_op))

    def urlformat(self, safe=':'):
        from urllib import quote
        return self.format(and_op='/', or_op=',', encoding=lambda q: quote(q, safe))

    @classmethod
    def urlscan(cls, string):
        from urllib import unquote
        return cls.scan(string, and_op='/', or_op=',', decoding=unquote)

    @classmethod
    def parse(cls, string):
        import re
        return eval(re.sub(r'(\w+)', r'Q.wrap("""\1""")', string) or 'Q([])')

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
        from operator import __and__
        return Q.wrap(reduce(__and__, (~l for l in self.literals)))

    def __eq__(self, other):
        return self.literals == other.literals

    def __hash__(self):
        return hash(self.literals)

    def __str__(self):
        return ' | '.join('%s' % l for l in self.literals)

    def format(self, or_op='|', not_op='~', encoding=str):
        return or_op.join(l.format(not_op=not_op, encoding=encoding) for l in self.literals)

    @classmethod
    def scan(cls, string, or_op='|', not_op='~', decoding=str):
        return Clause(Literal.scan(l, not_op=not_op, decoding=decoding) for l in string.split(or_op))

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
        return Literal(self.term, negated=not self.negated)

    def __eq__(self, other):
        return self.term == other.term and self.negated == other.negated

    def __hash__(self):
        return hash(self.term) ^ hash(self.negated)

    def __str__(self):
        return '%s%s' % ('~' if self.negated else '', self.term)

    def format(self, not_op='~', encoding=str):
        return '%s%s' % (not_op if self.negated else '', encoding(self.term))

    @classmethod
    def scan(cls, string, or_op='|', not_op='~', decoding=str):
        negated = string.startswith(not_op)
        return Literal(decoding(string.lstrip(not_op)), negated=negated)

if __name__ == '__main__':
    import doctest
    doctest.testmod()
