package mssql

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
	"time"

	//_ "github.com/alexbrainman/odbc"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/mitchellh/mapstructure"
)

// ProcArgs 储存过程参数
type ProcArgs struct {
	Name   string      //参数名
	Arg    interface{} //OUTPUT时传指针，否则传值
	Output bool        //参数是否为OUTPUT
}

var (
	dataBaseCache = &Mssql{}
)

type MssqlResult struct {
	rows []map[string]interface{}
}

type Mssql struct {
	*sql.DB
	dataSource string
	database   string
	dataport   string
	windows    bool
	sa         SA
}

type SA struct {
	user   string
	passwd string
}

//***********************************************************//

//func (m *Mssql) Open() (err error) {
//	var conf []string
//	conf = append(conf, "Provider=SQLOLEDB")
//	conf = append(conf, "Data Source="+m.dataSource)
//	if m.windows {
//		// Integrated Security=SSPI 这个表示以当前WINDOWS系统用户身去登录SQL SERVER服务器(需要在安装sqlserver时候设置)，
//		// 如果SQL SERVER服务器不支持这种方式登录时，就会出错。
//		conf = append(conf, "integrated security=SSPI")
//	}
//	conf = append(conf, "Initial Catalog="+m.database)
//	conf = append(conf, "user id="+m.sa.user)
//	conf = append(conf, "password="+m.sa.passwd)
//
//	m.DB, err = sql.Open("adodb", strings.Join(conf, ";"))
//	if err != nil {
//		return err
//	}
//	return nil
//}

func CharFilter(str string) string {
	var buf bytes.Buffer
	for _, val := range str {
		if val == '\'' {
			buf.WriteString(string(val))
		}
		buf.WriteString(string(val))
	}
	return buf.String()
}

func b2n(b bool) int {
	if b {
		return 1
	}
	return 0
}

func b2s(bs []uint8) string {
	ba := []byte{}
	for _, b := range bs {
		ba = append(ba, byte(b))
	}
	return string(ba)
}

func (m *Mssql) Open() (err error) {
	var conf []string

	conf = append(conf, "server="+m.dataSource)
	conf = append(conf, "port="+m.dataport)
	conf = append(conf, "database="+m.database)
	conf = append(conf, "user id="+m.sa.user)
	conf = append(conf, "password="+m.sa.passwd)
	conf = append(conf, "encrypt=disable")

	log.Println("open:", strings.Join(conf, ";"))
	m.DB, err = sql.Open("sqlserver", strings.Join(conf, ";"))
	//设置最大空闲连接数
	m.DB.SetMaxIdleConns(20)
	//设置最大连接数
	m.DB.SetMaxOpenConns(30)
	if err != nil {
		log.Println("open err:", err.Error())
		return err
	}
	return nil
}

// RowsCount 获取查询结果行数
func (r *MssqlResult) RowsCount() int {
	return len(r.rows)
}

func StringToFloatFunc() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{}) (interface{}, error) {

		if f.Kind() != reflect.String {
			return data, nil
		}
		if t.Kind() != reflect.Float64 && t.Kind() != reflect.Float32 {
			return data, nil
		}

		return strconv.ParseFloat(data.(string), 10)
	}
}

func TimeToStringFunc() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{}) (interface{}, error) {

		if f != reflect.TypeOf(time.Time{}) {
			return data, nil
		}

		if t.Kind() != reflect.String {
			return data, nil
		}
		ct := data.(time.Time)

		return ct.Format("2006-01-02 15:04:05"), nil
	}
}

func m2sDecode(input interface{}, result interface{}) error {
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Metadata: nil,
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			StringToFloatFunc(),
			TimeToStringFunc(),
		),
		Result: result,
	})
	if err != nil {
		return err
	}

	if err := decoder.Decode(input); err != nil {
		return err
	}
	return err
}

// GetRow 获取查询结果的index行内容
func (r *MssqlResult) GetRow(index int, v interface{}) bool {
	if index < 0 || index >= len(r.rows) {
		log.Println("Index Out of Bounds")
		return false
	}
	if err := m2sDecode(r.rows[index], v); err != nil {
		log.Println(err)
		return false
	}
	return true
}

// Init 初始化连接池
func Init(dataSource, dataport, database, user, passwd string) (*Mssql, error) {
	//dataBaseCache := &Mssql{}
	// 连接数据库

	ms := &Mssql{
		dataSource: dataSource,
		database:   database,
		dataport:   dataport,
		// windwos: true 为windows身份验证，false 必须设置sa账号和密码
		windows: false,
		sa: SA{
			user:   user,
			passwd: passwd,
		},
	}

	err := ms.Open()
	if err != nil {
		log.Println("sql open:", err)
		return nil, err
	}

	return ms, nil
}

// Execute 执行sql语句
func (m *Mssql) Execute(sSQL string) error {
	if m == nil {
		return errors.New("DB manager is nil")
	}
	fmt.Println("sql:", sSQL)

	//产生查询语句的Statement
	stmt, err := m.Prepare(sSQL)
	if err != nil {
		log.Println("[Execute] err1:", err)
		return err
	}
	defer stmt.Close()
	//通过Statement执行查询
	_, err = stmt.Exec()
	if err != nil {
		log.Println("[Execute] err2:", err)
		return err
	}
	return nil
}

// Query 执行查询语句
func (m *Mssql) Query(sSQL string) (*MssqlResult, error) {
	if m == nil {
		return nil, errors.New("DB manager is nil")
	}
	fmt.Println("sql:", sSQL)

	//产生查询语句的Statement
	stmt, err := m.Prepare(sSQL)
	if err != nil {
		log.Println("[Query] err1:", err)
		return nil, err
	}
	defer stmt.Close()
	//通过Statement执行查询
	rows, err := stmt.Query()
	if err != nil {
		log.Println("[Query] err2:", err)
		return nil, err
	}
	defer rows.Close()
	//建立一个列数组
	cols, err := rows.Columns()
	length := len(cols)
	value := make([]interface{}, length)
	columnPointers := make([]interface{}, length)
	for i := 0; i < length; i++ {
		columnPointers[i] = &value[i]
	}
	res := &MssqlResult{rows: make([]map[string]interface{}, 0)}
	//遍历每一行
	for rows.Next() {
		rows.Scan(columnPointers...)
		data := make(map[string]interface{})
		for i := 0; i < length; i++ {
			switch v := (*(columnPointers[i].(*interface{}))).(type) {
			case []uint8:
				//b, _ := columnPointers[i].([]uint8)
				data[cols[i]] = b2s([]uint8(v))
			default:
				data[cols[i]] = *columnPointers[i].(*interface{})
			}

			log.Println(*columnPointers[i].(*interface{}))
		}
		res.rows = append(res.rows, data)
	}
	return res, nil
}

//ExecuteWithOutput 执行带output的存储过程
/*
func (m *MssqlManages) ExecuteWithOutput(procname string, args ...interface{}) error {
	if m == nil {
		return errors.New("DB manager is nil")
	}

	fmt.Println(reflect.TypeOf(args))

	ms := m.AllocMssql()
	defer m.FreeMssql(ms)

	_, err := ms.ExecContext(context.Background(), procname, args...)
	if err != nil {
		log.Println("[ExecuteWithOutputArgs] err:", err)
		return err
	}

	return nil
}
*/

// ExecuteWithOutput 执行带output参数的存储过程
func (m *Mssql) ExecuteWithOutput(procname string, args []ProcArgs) error {

	if m == nil {
		return errors.New("DB manager is nil")
	}

	num := len(args)

	var argsReal []interface{}

	for i := 0; i < num; i++ {
		name := args[i].Name
		arg := args[i].Arg
		if args[i].Output {
			argsReal = append(argsReal, sql.Named(name, sql.Out{Dest: arg}))
		} else {
			argsReal = append(argsReal, sql.Named(name, arg))
		}
	}

	_, err := m.ExecContext(context.Background(), procname, argsReal...)
	if err != nil {
		log.Println("[ExecuteWithOutput] err:", err)
		return err
	}

	return nil
}

// QueryWithOutput 执行带select结果和output参数的存储过程
func (m *Mssql) QueryWithOutput(procname string, args []ProcArgs) (*MssqlResult, error) {

	if m == nil {
		return nil, errors.New("DB manager is nil")
	}

	num := len(args)

	var argsReal []interface{}

	for i := 0; i < num; i++ {
		name := args[i].Name
		arg := args[i].Arg
		if args[i].Output {
			argsReal = append(argsReal, sql.Named(name, sql.Out{Dest: arg}))
		} else {
			argsReal = append(argsReal, sql.Named(name, arg))
		}
	}

	rows, err := m.QueryContext(context.Background(), procname, argsReal...)
	if err != nil {
		log.Println("[QueryWithOutput] err:", err)
		return nil, err
	}

	defer rows.Close()
	//建立一个列数组
	cols, err := rows.Columns()
	length := len(cols)
	value := make([]interface{}, length)
	columnPointers := make([]interface{}, length)
	for i := 0; i < length; i++ {
		columnPointers[i] = &value[i]
	}
	res := &MssqlResult{rows: make([]map[string]interface{}, 0)}
	//遍历每一行
	for rows.Next() {
		rows.Scan(columnPointers...)
		data := make(map[string]interface{})
		for i := 0; i < length; i++ {
			switch v := (*(columnPointers[i].(*interface{}))).(type) {
			case []uint8:
				data[cols[i]] = b2s([]uint8(v))
			default:
				data[cols[i]] = *columnPointers[i].(*interface{})
			}

			//log.Println(*columnPointers[i].(*interface{}))
		}
		res.rows = append(res.rows, data)
	}
	return res, nil
}

/*
执行带select结果和output参数的存储过程存储过程
存储过程参数为 "@" + "参数名称",参数名称必须和存储过程参数名称保持一致。
示例：
-------存储过程--------
PROC [dbo].[API_Test] (

	@num INT,
	@str NVARCHAR(100),
	@num2 INT OUTPUT,
	@str2 NVARCHAR(100) OUTPUT

)
--------函数调用-------
StoredProcedureBySprint("exec API_Test @num,@str,@num2 output,@str2 output", num, str, &num2, &str2)
*/
func (m *Mssql) StoredProcedureBySprint(sSQL string, a ...interface{}) (*MssqlResult, error) {

	var (
		buf      bytes.Buffer
		flag     bool          = false
		lit      []interface{} = a
		n        int           = 0
		size     int           = len(lit)
		procname string        = ""
		args                   = make([]ProcArgs, 0)
		strs     []string      = strings.Split(strings.TrimSpace(sSQL), " ")
	)

	if len(strs) > 1 && strings.Compare(strings.ToLower(strs[0]), "exec") == 0 {
		procname = strs[1]
	} else {
		procname = strs[0]
	}

	if sSQL[len(sSQL)-1] != ',' {
		sSQL += ","
	}

	for _, val := range sSQL {
		if val == '@' {
			if n >= size {
				return nil, errors.New("参数错误")
			}

			flag = true
			buf.Reset()
			n++
		} else if val == ',' {
			if flag {
				strs = strings.Split(strings.TrimSpace(buf.String()), " ")
				if len(strs) == 2 {
					if strings.Compare(strings.ToLower(strs[1]), "output") == 0 ||
						strings.Compare(strings.ToLower(strs[1]), "out") == 0 {
						switch lit[n-1].(type) {
						case interface{}:
							args = append(args, ProcArgs{Name: strs[0], Arg: lit[n-1].(interface{}), Output: true})
						default:
							return nil, errors.New("参数错误")
						}
					} else {
						return nil, errors.New("参数错误")
					}
				} else if len(strs) == 1 {
					switch lit[n-1].(type) {
					case string:
						args = append(args, ProcArgs{Name: strs[0], Arg: CharFilter(lit[n-1].(string)), Output: false})
					case bool:
						args = append(args, ProcArgs{Name: strs[0], Arg: b2n(lit[n-1].(bool)), Output: false})
					default:
						args = append(args, ProcArgs{Name: strs[0], Arg: lit[n-1], Output: false})
					}
				} else {
					return nil, errors.New("参数错误")
				}
			}
			flag = false
		} else {
			buf.WriteString(string(val))
		}
	}

	return m.QueryWithOutput(procname, args)
}

/*
执行带select结果或output参数的存储过程存储过程（两者不可同时存在）,

	需要同时查询select结果集和output参数请使用 StoredProcedureBySprint 函数

存储过程参数为 "?" 输入参数,"!" 输出参数
示例：
-------存储过程--------
PROC [dbo].[API_Test] (

	@num INT,
	@str NVARCHAR(100),
	@num2 INT OUTPUT,
	@str2 NVARCHAR(100) OUTPUT

)
--------函数调用-------
QueryBySprint("exec API_Test ?,?,!,!", num, str, &num2, &str2)
*/
func (m *Mssql) QueryBySprint(sSQL string, a ...interface{}) (*MssqlResult, error) {
	var (
		buf      bytes.Buffer
		lit      []interface{} = a
		n        int           = 0
		size     int           = len(lit)
		declares []string      = make([]string, 0)
		keys     []string      = make([]string, 0)
		values   []interface{} = make([]interface{}, 0)
		name     string        = ""
	)

	for _, val := range sSQL {
		if val == '?' {
			if n >= size {
				return nil, errors.New("参数错误")
			}
			switch lit[n].(type) {
			case string:
				buf.WriteString(fmt.Sprintf("'%s'", CharFilter(lit[n].(string))))
			case bool:
				buf.WriteString(fmt.Sprintf("%d", b2n(lit[n].(bool))))
			default:
				buf.WriteString(fmt.Sprintf("%v", lit[n]))
			}
			n++
		} else if val == '!' {
			if n >= size {
				return nil, errors.New("参数错误")
			}

			name = fmt.Sprintf("@p%d", n)

			switch lit[n].(type) {
			case interface{}:
				switch (lit[n].(interface{})).(type) {
				case *string:
					declares = append(declares, fmt.Sprintf("declare %s nvarchar(512);set %s = '%s'", name, name, CharFilter(*(lit[n].(interface{})).(*string))))
				case *bool:
					declares = append(declares, fmt.Sprintf("declare %s int;set %s = %d", name, name, b2n(*(lit[n].(interface{})).(*bool))))
				case *float32:
					declares = append(declares, fmt.Sprintf("declare %s float;set %s = %v", name, name, *(lit[n].(interface{})).(*float32)))
				case *float64:
					declares = append(declares, fmt.Sprintf("declare %s float;set %s = %v", name, name, *(lit[n].(interface{})).(*float64)))
				case *int:
					declares = append(declares, fmt.Sprintf("declare %s int;set %s = %v", name, name, *(lit[n].(interface{})).(*int)))
				case *int32:
					declares = append(declares, fmt.Sprintf("declare %s int;set %s = %v", name, name, *(lit[n].(interface{})).(*int32)))
				case *int64:
					declares = append(declares, fmt.Sprintf("declare %s int;set %s = %v", name, name, *(lit[n].(interface{})).(*int64)))
				default:
					return nil, errors.New("参数错误")
				}
			default:
				return nil, errors.New("参数错误")
			}
			buf.WriteString(fmt.Sprintf("%s output", name))
			values = append(values, lit[n])
			keys = append(keys, fmt.Sprintf("%s as %s", name, name[1:]))
			n++
		} else {
			buf.WriteString(string(val))
		}
	}

	if len(declares) > 0 {
		sSQL = fmt.Sprintf("%s ;%s ;select %s", strings.Join(declares, ";"), buf.String(), strings.Join(keys, ","))
	} else {
		sSQL = buf.String()
		return m.Query(sSQL)
	}

	//产生查询语句的Statement
	stmt, err := m.Prepare(sSQL)
	if err != nil {
		log.Println("[Query] err1:", err)
		return nil, err
	}
	defer stmt.Close()
	//通过Statement执行查询
	rows, err := stmt.Query()
	if err != nil {
		log.Println("[Query] err2:", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(values...)
		if err != nil {
			return nil, err
		}
	}
	return &MssqlResult{}, nil
}
