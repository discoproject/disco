Style Guide for Disco Code
==========================

This document contains the coding conventions for the Disco codebase.

.. todo:: currently this is just an outline of things that should be included,
          lets start collecting these things here before we publish them

 - line width is 80 chars

 - lines should not be split if possible

 - if arguments are spread over multiple lines, they should be one per line

 - functions, keyword args and other unordered collections of things should
   be listed in alphabetical order unless there is some other overriding order

 - names of things should be clear, unabbreviated, and consistent

 - lines should not be dedented from the syntactic element they are part of

Erlang
------

Unless otherwise specified, `Erlang Programming Rules`_ apply.

.. _Erlang Programming Rules: http://www.erlang.se/doc/programming_rules.shtml

Also, the use of -spec annotations and Dialyzer is highly recommended
before checking in commits.

Python
------

Unless otherwise specified, guidelines from :pep:`8` apply.

Documentation
-------------

 - inline with code when possible
