// vim:ts=4:sw=4:et

package sqlany

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"math/rand"
	"testing"
)

// tests (mostly unmodified) courtesy of github.com/bmizerany/pq
type Fataler interface {
	Fatal(args ...interface{})
}

func openTestConn(t Fataler) *sql.DB {
	db, err := sql.Open("sqlany", "uid=dba;pwd=sql;dbf=test;eng=test")
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

// tests from Go sql test
// https://github.com/bradfitz/go-sql-test/blob/master/src/sqltest/sql_test.go
func sqlBlobParam(t params, size int) string {
	return fmt.Sprintf("VARBINARY(%d)", size)
}

type params struct {
	*testing.T
	*sql.DB
}

func (t params) mustExec(sql string, args ...interface{}) sql.Result {
	res, err := t.DB.Exec(sql, args...)
	if err != nil {
		t.Fatalf("Error running %q: %v", sql, err)
	}
	return res
}

func (t params) q(s string) string {
	return s // no-op
}

func TestBlobs(tst *testing.T) {
	db := openTestConn(tst)
	defer db.Close()
	t := params{tst, db}

	var blob = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	t.mustExec("CREATE TABLE #foo (id INTEGER PRIMARY KEY, bar " + sqlBlobParam(t, 16) + ")")
	t.mustExec(t.q("INSERT INTO #foo (id, bar) VALUES(?,?)"), 0, blob)

	want := fmt.Sprintf("%x", blob)

	b := make([]byte, 16)
	err := t.QueryRow(t.q("SELECT bar FROM #foo WHERE id = ?"), 0).Scan(&b)
	got := fmt.Sprintf("%x", b)
	if err != nil {
		t.Errorf("[]byte scan: %v", err)
	} else if got != want {
		t.Errorf("for []byte, got %q; want %q", got, want)
	}

	err = t.QueryRow(t.q("SELECT bar FROM #foo WHERE id = ?"), 0).Scan(&got)
	want = string(blob)
	if err != nil {
		t.Errorf("string scan: %v", err)
	} else if got != want {
		t.Errorf("for string, got %q; want %q", got, want)
	}
}

func TestManyQueryRow(tst *testing.T) {
	db := openTestConn(tst)
	defer db.Close()
	t := params{tst, db}

	if testing.Short() {
		t.Logf("skipping in short mode")
		return
	}
	t.mustExec("CREATE TABLE #foo (id INTEGER PRIMARY KEY, name VARCHAR(50))")
	t.mustExec(t.q("INSERT INTO #foo (id, name) VALUES(?,?)"), 1, "bob")
	var name string
	for i := 0; i < 10000; i++ {
		err := t.QueryRow(t.q("SELECT name FROM #foo WHERE id = ?"), 1).Scan(&name)
		if err != nil || name != "bob" {
			t.Fatalf("on query %d: err=%v, name=%q", i, err, name)
		}
	}
}

func TestTxQuery(tst *testing.T) {
	db := openTestConn(tst)
	defer db.Close()
	t := params{tst, db}

	tx, err := t.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	_, err = tx.Exec("CREATE TABLE #foo (id INTEGER PRIMARY KEY, name VARCHAR(50))")
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.Exec(t.q("INSERT INTO #foo (id, name) VALUES(?,?)"), 1, "bob")
	if err != nil {
		t.Fatal(err)
	}

	r, err := tx.Query(t.q("SELECT name FROM #foo WHERE id = ?"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if !r.Next() {
		if r.Err() != nil {
			t.Fatal(err)
		}
		t.Fatal("expected one rows")
	}

	var name string
	err = r.Scan(&name)
	if err != nil {
		t.Fatal(err)
	}
}

func TestPreparedStmt(tst *testing.T) {
	db := openTestConn(tst)
	defer db.Close()
	t := params{tst, db}

	t.mustExec("CREATE TABLE t (count INT)")
	sel, err := t.Prepare("SELECT count FROM t ORDER BY count DESC")
	if err != nil {
		t.Fatalf("prepare 1: %v", err)
	}
	ins, err := t.Prepare(t.q("INSERT INTO t (count) VALUES (?)"))
	if err != nil {
		t.Fatalf("prepare 2: %v", err)
	}

	for n := 1; n <= 3; n++ {
		if _, err := ins.Exec(n); err != nil {
			t.Fatalf("insert(%d) = %v", n, err)
		}
	}

	const nRuns = 10
	ch := make(chan bool)
	for i := 0; i < nRuns; i++ {
		go func() {
			defer func() {
				ch <- true
			}()
			for j := 0; j < 10; j++ {
				count := 0
				if err := sel.QueryRow().Scan(&count); err != nil && err != sql.ErrNoRows {
					t.Errorf("Query: %v", err)
					return
				}
				if _, err := ins.Exec(rand.Intn(100)); err != nil {
					t.Errorf("Insert: %v", err)
					return
				}
			}
		}()
	}
	for i := 0; i < nRuns; i++ {
		<-ch
	}
}
