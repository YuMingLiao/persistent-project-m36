# [Project:M36](http://hackage.haskell.org/package/project-m36) Database Driver for the [Persistent Library](http://hackage.haskell.org/package/persistent)

This package provides a driver to connect the [persistent library](http://hackage.haskell.org/package/persistent) to the [Project:M36 relational algebra engine](http://hackage.haskell.org/package/project-m36). The driver supports all standard persistent features.

Project:M36 supports many Haskell-specific features which this driver, due to limitations in persistent, cannot support such as:

* native algebraic data types as database values
* NULL-free operation
* joins (as well as all relational operators)
* server-side, interpreted, runtime-loadable, Haskell functions to operate on database values


