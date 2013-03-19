// vim:ts=4:sw=4:et

package sqlany

import (
    "database/sql"
    "database/sql/driver"
    "testing"
    "log"
)

// tests (mostly unmodified) courtesy of github.com/bmizerany/pq
type Fataler interface {
    Fatal(args ...interface{})
}

func openTestConn(t Fataler) *sql.DB {
    db, err := sql.Open("sqlany", "uid=dba;pwd=sql;dbf=test")
    if err != nil {
        t.Fatal(err)
    }
    return db
}

func TestExec(t *testing.T) {
    db := openTestConn(t)
    defer db.Close()

	_, err := db.Exec("CREATE TABLE #temp (a INT)")
	if err != nil {
		t.Fatal(err)
	}

	r, err := db.Exec("INSERT INTO #temp VALUES (1)")
	if err != nil {
		t.Fatal(err)
	}

	if n, _ := r.RowsAffected(); n != 1 {
		t.Fatalf("expected 1 row affected, not %d", n)
	}

    // unfortunately, Sybase (<12) does not support multirow inserts
    // so keeping this really simple
	r, err = db.Exec("INSERT INTO #temp VALUES (?)", 1)
	if err != nil {
		t.Fatal(err)
	}

	if n, _ := r.RowsAffected(); n != 1 {
		t.Fatalf("expected 1 row affected, not %d", n)
	}
}

func TestStatment(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	st, err := db.Prepare("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}

	st1, err := db.Prepare("SELECT 2")
	if err != nil {
		t.Fatal(err)
	}

	r, err := st.Query()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if !r.Next() {
		t.Fatal("expected row")
	}

	var i int
	err = r.Scan(&i)
	if err != nil {
		t.Fatal(err)
	}

	if i != 1 {
		t.Fatalf("expected 1, got %d", i)
	}

	// st1

	r1, err := st1.Query()
	if err != nil {
		t.Fatal(err)
	}
	defer r1.Close()

	if !r1.Next() {
		if r.Err() != nil {
			t.Fatal(r1.Err())
		}
		t.Fatal("expected row")
	}

	err = r1.Scan(&i)
	if err != nil {
		t.Fatal(err)
	}

	if i != 2 {
		t.Fatalf("expected 2, got %d", i)
	}
}

func TestRowsCloseBeforeDone(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	r, err := db.Query("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}

	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}

	if r.Next() {
		t.Fatal("unexpected row")
	}

	if r.Err() != nil {
		t.Fatal(r.Err())
	}
}

func TestNoData(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

    // sqla does not have booleans
	st, err := db.Prepare("SELECT 1 WHERE 1 = 0")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	r, err := st.Query()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if r.Next() {
		if r.Err() != nil {
			t.Fatal(r.Err())
		}
		t.Fatal("unexpected row")
	}
}

func TestSQLAError(t *testing.T) {
	// Don't use the normal connection setup, this is intended to
	// blow up in the startup packet from a non-existent user.
	db, err := sql.Open("sqlany", "uid=thisuserreallydoesntexist")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Begin()
	if err == nil {
		t.Fatal("expected error")
	}

	if err, ok := err.(*sqlaError); !ok {
		t.Fatalf("expected a *sqlaError, got: %v", err)
	}
}

func TestBindError(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	_, err := db.Exec("CREATE table #test (i INT)")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Query("SELECT * FROM #test WHERE i = ?", "hhh")
	if err == nil {
		t.Fatal("expected an error")
	}

	// Should not get error here
	r, err := db.Query("SELECT * FROM #test WHERE i = ?", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
}

func TestExecerInterface(t *testing.T) {
	// Gin up a straw man private struct just for the type check
	cn := &conn{cn: 0}
	var cni interface{} = cn

	_, ok := cni.(driver.Execer)
    // [ap]: inverted as sqlago does not yet implement Execer
	if ok {
		t.Fatal("Driver should not implement Execer")
	}
}

func TestNullAfterNonNull(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	r, err := db.Query("SELECT 9 UNION SELECT NULL")
	if err != nil {
		t.Fatal(err)
	}

	var n sql.NullInt64

	if !r.Next() {
		if r.Err() != nil {
			t.Fatal(err)
		}
		t.Fatal("expected row")
	}

	if err := r.Scan(&n); err != nil {
		t.Fatal(err)
	}

	if n.Int64 != 9 {
		t.Fatalf("expected 9, not %d", n.Int64)
	}

	if !r.Next() {
		if r.Err() != nil {
			t.Fatal(err)
		}
		t.Fatal("expected row")
	}

	if err := r.Scan(&n); err != nil {
		t.Fatal(err)
	}

	if n.Valid {
        log.Printf("%v", n)
		t.Fatal("expected n to be invalid")
	}

	if n.Int64 != 0 {
		t.Fatalf("expected n to 2, not %d", n.Int64)
	}
}

