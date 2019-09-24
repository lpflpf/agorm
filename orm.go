package agorm

import (
	"database/sql"
	"fmt"
	"go/ast"
	"reflect"
	"sync"
	"unicode"
)

// database setting
type Orm struct {
	Host              string `json:"host"`
	Port              int    `json:"port"` // default 3301
	User              string `json:"user"`
	Pass              string `json:"pass"`
	Charset           string `json:"charset"` // utf8mb
	DatabaseName      string `json:"database"`
	MaxIdleConnection int    `json:"maxIdle"`
	MaxOpenConnection int    `json:"maxOpen"`
	MaxTimeout        int    `json:"timeout"`     // 30s
	MaxReadTimeout    int    `json:"readTimeout"` // 300s
	sync.Mutex
	Db *sql.DB
}

var structMappingContainer = sync.Map{}

func getStructMapping(reflectType reflect.Type) map[string]int {
	if value, ok := structMappingContainer.Load(reflectType); !ok {
		value = setTagMaps(reflectType)
		structMappingContainer.Store(reflectType, value)
		return value.(map[string]int)
	} else {
		return value.(map[string]int)
	}
}

func (o *Orm) query(query string, args ...interface{}) (*sql.Rows, error) {
	if o.Db == nil {
		// lazy connect
		if err := tryAgain(o.connect); err != nil {
			return nil, err
		}
	}

	var err error
	var rows *sql.Rows

	err = tryAgain(func() error {
		rows, err = o.Db.Query(query, args...)
		if err != nil && err.Error() == "sql: database is closed" {
			_ = o.reConnect()
		}

		return err
	})

	return rows, err
}

func (o *Orm) connect() error {
	o.Lock()
	if o.Db != nil {
		return nil
	}

	if o.MaxTimeout == 0 {
		o.MaxTimeout = 30
	}

	if o.MaxReadTimeout == 0 {
		o.MaxReadTimeout = 300
	}

	if o.Charset == "" {
		o.Charset = "utf8mb4"
	}

	if o.Port == 0 {
		o.Port = 3301
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?timeout=%ds&readTimeout=%ds&charset=%s",
		o.User,
		o.Pass,
		o.Host,
		o.Port,
		o.DatabaseName,
		o.MaxTimeout,
		o.MaxReadTimeout,
		o.Charset,
	)
	var db *sql.DB
	db, err := sql.Open("mysql", dsn)

	if err != nil {
		Logger.Printf("connect %s error: %s", o.DatabaseName, err)
		return err
	} else {
		db.SetMaxIdleConns(o.MaxIdleConnection)
		db.SetMaxOpenConns(o.MaxOpenConnection)
		o.Db = db
	}
	defer o.Unlock()

	return nil
}

func (o *Orm) reConnect() error {
	o.Lock()
	defer o.Lock()

	if o.Db != nil {
		_ = o.Db.Close()
	}

	return o.connect()
}

func (o *Orm) QueryRow(result interface{}, query string, args ...interface{}) error {
	rows, err := o.query(query, args...)
	defer func() { _ = rows.Close() }()

	if err != nil {
		return err
	}

	if !rows.Next() {
		return ErrNoData
	}

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	resultValue := reflect.ValueOf(result)

	if resultValue.Kind() == reflect.Ptr {
		resultValue = resultValue.Elem()
	} else {
		// must be pointer
		return ErrParamMustBeStructPointer
	}

	maps := getStructMapping(resultValue.Type())
	var ignored interface{}
	var scanArgs []interface{}

	for _, column := range columns {
		if idx, ok := maps[column]; !ok {
			scanArgs = append(scanArgs, &ignored)
		} else {
			scanArgs = append(scanArgs, resultValue.Field(idx).Addr().Interface())
		}
	}

	return rows.Scan(scanArgs...)
}

func (o *Orm) QueryRows(result interface{}, query string, args ...interface{}) error {
	rows, err := o.query(query, args...)
	defer func() { _ = rows.Close() }()

	if err != nil {
		return err
	}

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	resultValue := reflect.ValueOf(result)

	if resultValue.Kind() != reflect.Ptr {
		return ErrParamMustBeSlicePointer
	}
	resultSlice := resultValue.Elem()

	if resultSlice.Kind() != reflect.Slice {
		return ErrParamMustBeSlicePointer
	}

	elementType := resultSlice.Type().Elem()

	if elementType.Kind() != reflect.Struct {
		return ErrParamMustBeSlicePointer
	}

	maps := getStructMapping(elementType)

	element := reflect.New(elementType).Elem()
	var ignored interface{}
	var scanArgs []interface{}

	for _, column := range columns {
		if idx, ok := maps[column]; !ok {
			scanArgs = append(scanArgs, &ignored)
		} else {
			scanArgs = append(scanArgs, element.Field(idx).Addr().Interface())
		}
	}

	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return err
		}
		resultSlice.Set(reflect.Append(resultSlice, element))
	}
	return nil
}

func setTagMaps(reflectType reflect.Type) map[string]int {
	tagMap := map[string]int{}

	for i := 0; i < reflectType.NumField(); i++ {
		fieldStruct := reflectType.Field(i)
		if !ast.IsExported(fieldStruct.Name) {
			continue
		}

		str := fieldStruct.Tag.Get(TagName)

		if str == "" {
			tag := lcFirst(fieldStruct.Name)
			tagMap[tag] = i
			continue
		} else if str == "-" {
			continue
		} else {
			tagMap[str] = i
		}
	}

	return tagMap
}

func (o *Orm) Exec() {

}

func lcFirst(str string) string {
	if len(str) > 0 {
		runes := []rune(str)
		firstRune := unicode.ToLower(runes[0])
		runes[0] = firstRune
		return string(runes)
	}
	return str
}
