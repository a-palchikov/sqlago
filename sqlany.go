// vim:ts=4:sw=4:et

package sqlany

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log"
	"reflect"
	"syscall"
	"unsafe"
)

var (
	ErrNotSupported = errors.New("sqla: not supported")
)

func init() {
	sql.Register("sqlany", &drv{})
	sqlaInit("sqlago")
}

// database driver
type drv struct {
}

func (d *drv) Open(opts string) (_ driver.Conn, err error) {
	h := newConnection()
	// [ap]: augment the connection options string to instruct the server
	// to perform character set conversions and return strings in utf-8
	err = h.connect(opts + ";cs=utf8")
	if err != nil {
		return
	}
	c := &conn{cn: h, connected: true, charset: "utf-8"}
	// query the character set
	var cs string
	if err = c.queryRow("select connection_property('CharSet')", &cs); err == nil {
		c.charset = cs
	}
	return c, err
}

type conn struct {
	cn        sqlaConn // low-level connection handle
	t         *tx
	connected bool
	charset   string
}

type tx struct {
	cn *conn
}

// Connection interface
func (cn *conn) Begin() (driver.Tx, error) {
	_, err := cn.cn.executeDirect("BEGIN TRAN")
	if err != nil {
		return nil, err
	}
	return &tx{cn: cn}, nil
}

func (cn *conn) Close() error {
	if !cn.cn.disconnect() {
		log.Print("sqla: error disconnecting")
	}

	cn.cn.free()
	cn.connected = false
	return nil
}

func (cn *conn) Prepare(query string) (driver.Stmt, error) {
	st, err := cn.cn.prepare(query)
	if err != nil {
		return nil, err
	}
	numparams := st.numParams()
	stmt := &stmt{st: st, cn: cn, numparams: numparams}
	if numcols := st.numCols(); numcols > 0 {
		colinfo := &columnInfo{}
		cols := make([]string, numcols)
		for i := 0; i < numcols; i++ {
			if ok := st.getColumnInfo(sacapi_u32(i), colinfo); !ok {
				err := cn.cn.newError()
				return nil, err
			}
			cols[i] = colinfo.Name()
		}
		stmt.cols = cols
	}
	return stmt, nil
}

// Special purpose restricted query implementation that only knows
// about strings/ints
//
// It is used to query attributes such as `character set` and
// `last insert id` internally which otherwise would rely on much of
// the functionality which is currently unfortunately an implementation
// detail of Go's database package (database.Rows and database.Rows.Scan
// for instance).
//
// Imagine having to use database/sql inside of the driver implementation
// and you'll get the idea
func (cn *conn) queryRow(query string, args ...interface{}) (err error) {
	st, err := cn.cn.executeDirect(query)
	if err != nil {
		return
	}
	defer st.free()
	if ok := st.fetchNext(); !ok {
		return io.EOF
	}
	if numcols := st.numCols(); numcols > 0 {
		data := &dataValue{}
		for i := 0; i < numcols; i++ {
			if ok := st.getColumn(uint(i), data); !ok {
				err = cn.cn.newError()
				return
			}
			switch s := data.Value().(type) {
			case string:
				switch d := args[i].(type) {
				case *string:
					*d = string(s)
				}
			case uint64:
				switch d := args[i].(type) {
				case *uint64:
					*d = uint64(s)
				}
			}
		}
	}
	return
}

// optional Execer interface for one-shot queries
// TODO(ap): to be able to implement this correctly, I need to differentiate
// between queries that do not return a resultset (as executeImmediately expects)
// No other way to do that (to still be able to fallback to default behaviour)
// than checking if a query is a `DELETE` or `UPDATE` for instance - meaah
/*
func (cn *conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if len(args) == 0 {
		err := cn.cn.executeImmediate(query)
		if err != nil {
			return nil, err
		}
		return &result{}, nil
	}
	// return ErrSkip to run the default implementation
	return nil, driver.ErrSkip
}
*/

// Tx
func (t *tx) Commit() error {
	if ret := t.cn.cn.commit(); !ret {
		return t.cn.cn.newError()
	}
	return nil
}

func (t *tx) Rollback() error {
	if ret := t.cn.cn.rollback(); !ret {
		return t.cn.cn.newError()
	}
	return nil
}

type result struct {
	st          *stmt
	numaffected int64
}

func (res *result) RowsAffected() (int64, error) {
	return res.numaffected, nil
}

func (res *result) LastInsertId() (int64, error) {
	if res.st != nil {
		var id uint64
		if err := res.st.cn.queryRow("select @@identity", &id); err != nil {
			return 0, err
		}
		return int64(id), nil
	}
	return 0, ErrNotSupported
}

type stmt struct {
	cn        *conn
	st        sqlaStmt
	query     string
	cols      []string
	numparams int
	closed    bool
}

// Statements
//
func (st *stmt) Close() error {
	if st.closed {
		log.Print("stmt.Close: invoked on an already closed stmt")
		return nil
	}
	if st.st.numCols() > 0 {
		st.st.reset()
		/* if isAutoCommit {
		    _ = st.cn.cn.commit()   // ignore the result
		} */
	}
	st.st.free()
	st.closed = true
	return nil
}

func (st *stmt) execute(args []driver.Value) (err error) {
	if st.st.numCols() > 0 {
		// auto-commit if configured
		st.st.reset()
	}
	if args != nil {
		if len(args) != st.numparams {
			return fmt.Errorf("Number of arguments do not match that of bind params provided (%d != %d)",
				len(args), st.numparams)
		}
		for i := 0; i < st.numparams; i++ {
			st.bindParam(uint(i), args[i])
		}
	}
	if ok := st.st.execute(); !ok {
		err = st.cn.cn.newError()
		return
	}
	return nil
}

func (st *stmt) bindParam(index uint, param interface{}) (err error) {
	bp := &bindParam{}
	idx := sacapi_u32(index)
	if ok := st.st.describeBindParam(idx, bp); !ok {
		err = st.cn.cn.newError()
		return
	}
	// FIXME(ap): handle param being nil
	isnull := param == nil
	bp.value.isnull = &isnull
	datasize := reflect.TypeOf(param).Size()
	// initial approximation
	bp.value.buffersize = datasize
	bp.value.length = &datasize
	v := reflect.ValueOf(param)
	switch v.Kind() {
	case reflect.Bool:
		var b [1]byte
		if v.Bool() {
			b[0] = 1
		} else {
			b[0] = 0
		}
		bp.value.buffer = &b[0]
		bp.value.datatype = A_UVAL8
	case reflect.Int64:
		i := v.Int()
		bp.value.buffer = (*byte)(unsafe.Pointer(&i))
		bp.value.datatype = A_VAL64
	case reflect.Int32, reflect.Int:
		i := int32(v.Int())
		bp.value.buffer = (*byte)(unsafe.Pointer(&i))
		bp.value.datatype = A_VAL32
	case reflect.Int16:
		i := int16(v.Int())
		bp.value.buffer = (*byte)(unsafe.Pointer(&i))
		bp.value.datatype = A_VAL16
	case reflect.Int8:
		i := int8(v.Int())
		bp.value.buffer = (*byte)(unsafe.Pointer(&i))
		bp.value.datatype = A_VAL8
	case reflect.Float32, reflect.Float64:
		f := v.Float()
		bp.value.buffer = (*byte)(unsafe.Pointer(&f))
		bp.value.datatype = A_DOUBLE
	case reflect.Complex64, reflect.Complex128:
	case reflect.String:
		bp.value.datatype = A_STRING
		s := v.String()
		b := syscall.StringBytePtr(s)
		size := uintptr(len(s))
		bp.value.buffer = b
		bp.value.buffersize = size + 1 // account for null terminator
		bp.value.length = &size
	case reflect.Slice:
		if b, ok := v.Interface().([]byte); ok {
			bp.value.datatype = A_BINARY
			bp.value.buffer = &b[0]
			size := uintptr(v.Len())
			bp.value.buffersize = size
			bp.value.length = &size
		}
		// FIXME(ap): fallthrough for non-byte slices
	default:
		log.Println("sqla: unsupported type", v)
		return ErrNotSupported
	}
	if ok := st.st.bindParam(idx, bp); !ok {
		err = st.cn.cn.newError()
		return
	}

	return nil
}

func (st *stmt) Query(args []driver.Value) (driver.Rows, error) {
	if err := st.execute(args); err != nil {
		return nil, err
	}
	return &rows{st: st}, nil
}

func (st *stmt) Exec(args []driver.Value) (driver.Result, error) {
	if err := st.execute(args); err != nil {
		return nil, err
	}
	numrows := st.st.affectedRows()
	r := &result{st: st, numaffected: int64(numrows)}
	return r, nil
}

func (st *stmt) NumInput() int {
	return st.st.numParams()
}

type rows struct {
	st *stmt
}

func (rs *rows) Close() error {
	return nil
}

func (rs *rows) Columns() []string {
	return rs.st.cols
}

func (rs *rows) Next(dest []driver.Value) (err error) {
	if ok := rs.st.st.fetchNext(); !ok {
		if err = rs.st.cn.cn.newError(); err != nil {
			code := err.(*sqlaError).code
			// check if the result set has really been exhausted
			if code != 100 {
				return
			}
		}
		return io.EOF
	}
	if numcols := rs.st.st.numCols(); numcols > 0 {
		data := &dataValue{}
		for i := 0; i < numcols; i++ {
			if ok := rs.st.st.getColumn(uint(i), data); !ok {
				err = rs.st.cn.cn.newError()
				return // simply abandon the result set?
			}
			dest[i] = data.Value()
		}
	}
	return nil
}
