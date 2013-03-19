# sqlago - minimalistic Go driver for Sybase SQL Anywhere

sqlago driver is a wrapper over SQL Anywhere C API

## Installation
  go get github.com/a-palchikov/sqlago

## Examples of use:

    package main
    
    import (
        _ "github.com/a-palchikov/sqlago"
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
    
See http://dcx.sybase.com/index.html#1201/en/dbadmin/how-introduction-connect.html for detailed reference.

## Tested on

Windows 7 Pro 64bit

