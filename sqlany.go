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

func (d *drv) Open(opts string) (cn driver.Conn, err error) {
	//log.Printf("sqla: open('%s')\n", opts)
	h := newConnection()
	err = h.connect(opts)
	if err != nil {
		return
	}
	cn = &conn{cn: h, connected: true}
	// query the character set
	// TODO(ap)
	//cn.Exec("select connection_property('CharSet')", nil)
	return
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

// optional Execer interface for one-shot queries
/*
func (cn *conn) Exec(query string, args []driver.Value) (driver.Result, error) {
    if len(args) == 0 {
        st, err := cn.cn.executeDirect(query)
        stmt := &stmt{st: st, cn: cn, numparams: 0}
        if err != nil {
            return nil, err
        }
        numaffected := st.affectedRows()
        return &result{st: stmt, numaffected: int64(numaffected)}, nil
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
	/*
	   rows, err := res.st.Query("select @@identity")
	   rows.Next()
	   var id int
	   err = rows.Scan(&id)
	   if err != nil {
	       return -1, errors.New("sqla: unable to query last insert id")
	   }
	   //TODO(ap): maybe implement
	*/
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
	st.st.free()
	st.closed = true
	return nil
}

func (st *stmt) execute(args []driver.Value) (err error) {
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
	default:
		log.Println("sqla: unsupported type", v)
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
	r := &result{numaffected: int64(numrows)}
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
