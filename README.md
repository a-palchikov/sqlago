# sqlago - minimalistic Go driver for Sybase SQL Anywhere

## Installation
  go get github.ocm/a-palchikov/sqlago

## Examples of use:

package main

import (
    _ "github.ocm/a-palchikov/sqlago"
    "database/sql"
    "log"
)

func main() {
    db, err := sql.Open("sqlany", "uid=dba;pwd=sql;eng=myengine")
    if err != nil {
        log.Fatalf("Unable to connect to db: %s", err)
    }
    // Run basic query
    var name
    err = db.QueryRow("select name from users where user_id is not null").Scan(&name)
    if err != nil {
        log.Fatalf("Select failed: %s", err)
    }
    // Run query with multiple return rows
    rows, err = db.Query("select version, product, uid from myproducts")
    if err != nil {
        log.Fatalf("Select failed: %s", err)
    }
    for rows.Next() {
        var version float64
        var product, uid string
        err = rows.Scan(&version, &product, &uid)
        if err != nil {
            log.Fatalf("Select failed: %s", err)
        }
        log.Printf("version: %0.2f, product: %s, uid: %s", version, product, name)
    }
}

## Connection string

Connection string format is the format ubiquitously accepted by SQLA toolset:
    attr1=value1;attr2=value2...

sqlgo is a minimalistic wrapper over sqla DB C API library.
Currently only tested on windows but I assume porting to Linux/OS X platforms is easy.
