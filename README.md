# sqlago - minimalistic pure Go driver for Sybase SQL Anywhere

sqlago driver is a wrapper over SQL Anywhere C API

## Installation
  go get github.com/a-palchikov/sqlago

## Examples of use:
```go
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
        var name string
        err = db.QueryRow("select name from users").Scan(&name)
        if err != nil {
            log.Fatalf("Select failed: %s", err)
        }
        // Run query with multiple return rows
        rows, err = db.Query("select version, product, uid from products")
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
```
### Using prepared statements: 
```go
    func preparedQuery(db *sql.DB) {
        st, err := db.Prepare("select langid from language where name = :name")
        if err != nil {
            log.Fatalln("Failed to prepare statement", err)
        }
        defer st.Close()    // explicit Close() required for statements obtained with Prepare()
        langs := []string{"english", "chinese"}
        var langid int
        for _, v := range langs {
            st.QueryRow(v).Scan(&langid)
            if err != nil {
                // non-fatal
                log.Printf("Failed to retrieve language id for '%s': %s", v, err)
            }
            log.Println("Language id", langid)
        }
    }
```

## Connection string

Connection string format is the format ubiquitously accepted by SQLA toolset:

    attr1=value1;attr2=value2...
    
See http://dcx.sybase.com/index.html#1201/en/dbadmin/how-introduction-connect.html for detailed reference.

## Testing

An accompanying `boostrap_test.cmd` batch file assumes SQL Anywhere 11 installation - edit it with the path to your installation
in case it differs.

Invoke the bootstrap batch to create an empty database, followed by `go test`.


## Tested on

Windows 7 Pro 64bit

