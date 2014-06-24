Style Guide for Disco Code
==========================

This document contains the coding conventions for the Disco codebase.

- line width is 80 chars

- lines should not be split if possible

- if arguments are spread over multiple lines, they should be one per line

- functions, keyword args and other unordered collections of things should
  be listed in alphabetical order unless there is some other overriding order

- names of things should be clear, unabbreviated, and consistent

- lines should not be dedented from the syntactic element they are part of

- Each commit should have a single purpose.  For example, if you are using the word
  'and' in the commit message, think about splitting the commit before submission.
 
- All of the commit messages should be imperative.  For example, instead of
  "adding ..." or "added ..." please use "add ...".  Think of the commit message as a
  function that is applied to the code base.
   
- The code should compile fine and all of the tests should pass with any commit.

- Do not push code directly into the main repo.  Please fork this repo, modify it and
  then create a pull request.  This way, the code is reviewed before being merged in,
  we can run the integration tests on them, and there is a merge commit that will have
  helpful information.
  
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
