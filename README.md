# Golang HBase client [![CI](https://github.com/tsuna/gohbase/actions/workflows/ci.yml/badge.svg)](https://github.com/tsuna/gohbase/actions/workflows/ci.yml) [![codecov.io](http://codecov.io/github/tsuna/gohbase/coverage.svg?branch=master)](http://codecov.io/github/tsuna/gohbase?branch=master) [![GoDoc](https://godoc.org/github.com/tsuna/gohbase?status.png)](https://godoc.org/github.com/tsuna/gohbase)

This is a pure [Go](http://golang.org/) client for [HBase](http://hbase.org).

Current status: beta.

## Supported Versions

HBase >= 1.0

## Installation

    go get github.com/tsuna/gohbase

## Example Usage

#### Create a client
```go
client := gohbase.NewClient("localhost")
```
#### Insert a cell
```go
// Values maps a ColumnFamily -> Qualifiers -> Values.
values := map[string]map[string][]byte{"cf": map[string][]byte{"a": []byte{0}}}
putRequest, err := hrpc.NewPutStr(context.Background(), "table", "key", values)
rsp, err := client.Put(putRequest)
```

#### Get an entire row
```go
getRequest, err := hrpc.NewGetStr(context.Background(), "table", "row")
getRsp, err := client.Get(getRequest)
```

#### Get a specific cell
```go
// Perform a get for the cell with key "15", column family "cf" and qualifier "a"
family := map[string][]string{"cf": []string{"a"}}
getRequest, err := hrpc.NewGetStr(context.Background(), "table", "15",
    hrpc.Families(family))
getRsp, err := client.Get(getRequest)
```

#### Get a specific cell with a filter
```go
pFilter := filter.NewKeyOnlyFilter(true)
family := map[string][]string{"cf": []string{"a"}}
getRequest, err := hrpc.NewGetStr(context.Background(), "table", "15",
    hrpc.Families(family), hrpc.Filters(pFilter))
getRsp, err := client.Get(getRequest)
```

#### Scan with a filter
```go
pFilter := filter.NewPrefixFilter([]byte("7"))
scanRequest, err := hrpc.NewScanStr(context.Background(), "table",
		hrpc.Filters(pFilter))
scanRsp, err := client.Scan(scanRequest)
```

## HBase 2.x administration

Create, enable, disable, delete, and replication table filters accept either
`table` (default namespace) or `namespace:table`. Namespaces must already exist.

The fork adds `AdminClient.AddColumnFamily`, `ModifyColumnFamily`, and
`TruncateTable`. Their request constructors return `(request, error)`:

```go
add, err := hrpc.NewAddColumnFamily(ctx, []byte("tenant:materials"),
    &pb.ColumnFamilySchema{Name: []byte("TypeDatahbase")})
if err != nil {
    return err
}
if err := admin.AddColumnFamily(add); err != nil {
    return err
}
```

Import `github.com/tsuna/gohbase/pb` for descriptors. For modification, fetch the
existing descriptor with `GetTableSchema`, change the desired attributes, and pass
the complete descriptor to `hrpc.NewModifyColumnFamily`. This replaces a descriptor;
it does not merge an attribute patch. Constructors copy table/descriptor input and
keep request nonces stable across retries.

To truncate, explicitly disable the table, then construct
`hrpc.NewTruncateTable(ctx, table, preserveSplits)` and call `admin.TruncateTable`.
Truncation removes all rows and HBase re-enables the table on completion.
The client does not enforce application permissions such as `Truncatable`.

These three new methods require HBase 2.x procedure IDs and wait for completion.
An absent ID returns an error rather than reporting premature success. Supply a
deadline: cancellation stops waiting but cannot undo a server-side operation.
Reuse the same request to retain its retry identity.

Call `Close()` on admin clients as well as data clients. Admin shutdown cancels
master dial work, releases the connection, and rejects later RPCs. Scanner cleanup
is asynchronous and uses a separate five-second deadline, allowing a close attempt
after scan cancellation without retrying indefinitely. These lifecycle changes
have regression tests added; execution and live failure validation are pending.

The live integration test creates and cleans up unique test tables:

```sh
HBASE_TEST_NAMESPACE=test_ns go test -tags=integration \
    -run '^TestAdminColumnFamiliesAndTruncate$' -timeout=5m -args -host=localhost
```

Use a test cluster and an existing namespace. Omit `HBASE_TEST_NAMESPACE` to use
the default namespace. Both split-preservation modes are tested. Wire fields were
checked against Apache HBase 2.0.4; live compatibility still needs validation.

## Contributing

Any help would be appreciated. Please use Github pull requests
to send changes for review. Please sign the
[Contributor License Agreement](https://docs.google.com/spreadsheet/viewform?formkey=dFNiOFROLXJBbFBmMkQtb1hNMWhUUnc6MQ)
when you send your first change for review.  

## License

Copyright © 2015 The GoHBase Authors. All rights reserved. Use of this source code is governed by the Apache License 2.0 that can be found in the [COPYING](COPYING) file.
