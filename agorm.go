package agorm

import (
	"encoding/json"
	"errors"
	_ "github.com/go-sql-driver/mysql"
	"io/ioutil"
	"log"
	"os"
	"sync"
)

type agorm struct {
	sync.RWMutex

	allOrm         map[string]*Orm
	router         func(args ...interface{}) string
	modelStructMap sync.Map
}

var (
	dbs                         agorm
	Logger                      = log.New(os.Stderr, " [agorm] ", log.Flags())
	TagName                     = "orm"
	ErrDbsNotEmpty              = errors.New("cannot init orm by config file, because orm has been initialized")
	ErrParamMustBeStructPointer = errors.New("param must be a struct pointer")
	ErrParamMustBeSlicePointer  = errors.New("param must be a slice pointer")
	ErrNoData                   = errors.New("empty return")
)

func init() {
	dbs.allOrm = make(map[string]*Orm)
}

func SetDbConfig(pathname string) error {
	dbs.Lock()
	defer dbs.Unlock()

	if len(dbs.allOrm) != 0 {
		return ErrDbsNotEmpty
	}

	data, err := ioutil.ReadFile(pathname)

	if err != nil {
		return err
	}

	return json.Unmarshal(data, &dbs.allOrm)
}

// register database by manual
func RegisterDatabase(alias, databaseName, user, pass, charset, host string, port, maxIdle, maxOpen int) {
	dbs.Lock()
	dbs.Unlock()

	dbs.allOrm[alias] = &Orm{
		Host:              host,
		Port:              port,
		User:              user,
		Pass:              pass,
		Charset:           charset,
		DatabaseName:      databaseName,
		MaxIdleConnection: maxIdle,
		MaxOpenConnection: maxOpen,
	}
}

func Using(alias string) *Orm {
	dbs.RLock()
	defer dbs.RUnlock()
	return dbs.allOrm[alias]
}

// set a default route func for select database
func SetAutoRouteFunc(caller func(args ...interface{}) string) {
	dbs.Lock()
	defer dbs.Unlock()

	dbs.router = caller
}

func Route(args ...interface{}) *Orm {
	return Using(dbs.router(args...))
}
