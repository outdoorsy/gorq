gorq
============

### WARNING:

This extension is very new.  It was originally written directly in the
gorp code base, but as it grew, I decided it would be best to submit
it as a separate package.  I have it in a place, currently, where it
appears to work for sqlite, MySQL, and PostgreSQL.  However, I *only*
use PostgreSQL, myself, so if you use sqlite or MySQL, you'll be
relying on the test coverage to ensure that I don't break anything.

Test coverage is pretty poor right now, so if you start using this and
run in to problems, please submit an issue, preferably with a pull
request that includes a failing test.  I will be adding tests over
time, as well - my goal is to get to (and stay at) 80% coverage; I
just don't have as much time as I'd like.

## About

gorq extends [gorp](https://github.com/go-gorp/gorp) with a query
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
go get github.com/outdoorsy/gorq
```

```go
import "github.com/outdoorsy/gorq"
import _ "github.com/lib/pq"
```

```go
dbMap, err := gorq.Open("postgres", "postgresql://hostname/dbname", nil)
defer dbMap.Close()

// Example query
ref := &Model{}
results, err := dbMap.Select(ref).
    Where().
    Equal(&ref.Id, testId).
    Select()
```

`gorq.DbMap` includes all of the functionality of
`gorp.DbMap`, with a few extensions.  See
[the documentation for gorq](http://godoc.org/github.com/outdoorsy/gorq)
for details on the extensions.  See
[the documentation for gorp](http://godoc.org/github.com/go-gorp/gorp)
for details on the functionality provided by gorp.
