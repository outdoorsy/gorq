[![GoDoc](https://godoc.org/github.com/nelsam/gorp_queries?status.png)](http://godoc.org/github.com/nelsam/gorp_queries)


gorp_queries
============

### WARNING:

*This extension is not ready yet.  It was originally
created as a patch directly to gorp, and is in the process of
being moved and restructured as an extension.  Everything (even
the repository name) is subject to change until further notice.*

## About

gorp_queries extends [gorp](github.com/coopernurse/gorp) with a query
DSL intended to catch SQL mistakes at compile time instead of runtime.
This is accomplished using reference structs and a relatively
complicated interface structure.

### Goals

1. Use the existing table<->struct map to find column names for use in
where clauses.  This way, any changes to column names or table structure
will only need to be added to code once - in the "db" field tag or table
mapping code.
2. Ensure that any form of spelling mistake is caught at compile time.
3. Allow a cascading method call structure that is familiar to anyone
who knows SQL, and enforce a sane order in any cascading method calls.
4. In cases where the query is built inside of if statements and for
loops, allow methods to be called in whichever order makes the most sense
to the programmer.

### Getting Started

Use go get and import the package:

```bash
go get github.com/nelsam/gorp_queries
```

```go
import "github.com/nelsam/gorp_queries"
```

Then, set up your DB map using `gorp_queries.DbMap` and use gorp as
normal:

```go
var dbMap = new(gorp_queries.DbMap)
dbMap.Db = dbConnection
dbMap.Dialect = dbDialect

// Example query
ref := new(Model)
results, err := dbMap.Select(ref).
    Where().
    Equal(&ref.Id, testId).
    Select()
```

`gorp_queries.DbMap` includes all of the functionality of
`gorp.DbMap`, with a few extensions.  See
[the documentation for gorp_queries](http://godoc.org/github.com/nelsam/gorp_queries)
for details on the extensions.  See
[the documentation for gorp](http://godoc.org/github.com/coopernurse/gorp)
for details on the functionality provided by gorp.
