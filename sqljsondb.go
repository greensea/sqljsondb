package sqljsondb

import (
    "fmt"
    "reflect"
    "sync"
    "time"
    "encoding/json"
    "errors"
    
    "github.com/json-iterator/go"
    "github.com/gohouse/gorose/v2"
    "github.com/cespare/xxhash"
)

type Driver struct {
    DSN string
    DBEngine *gorose.Engin
    TablePrefix string
    mu sync.Mutex
    
    ExistsCol sync.Map
    
    AfterUpdateFunc func(col string, key string, i interface{})
}

/// dsn is the DSN connection string. 
/// prefix is the table prefix.
func New(dsn string, prefix string) (*Driver, error){
    return &Driver{
        DSN: dsn,
        TablePrefix: prefix,
    }, nil
}

func (t *Driver) DB() gorose.IOrm {
    var err error
    
    if t.DBEngine != nil {    
        return t.DBEngine.NewOrm()
    }
    
    t.mu.Lock()
    defer t.mu.Unlock()
    
    if t.DBEngine != nil {
        return t.DBEngine.NewOrm()
    }
    
    var cnt time.Duration = 1
    for {
        t.DBEngine, err = gorose.Open(&gorose.Config{
            Driver: "mysql",
            Dsn: t.DSN,
            SetMaxOpenConns: 100,
            SetMaxIdleConns: 10,
        })
        
        if err != nil {
            cnt = cnt + 1
            if cnt > 60  {
                cnt = 60
            }
            fmt.Printf("%s. Retry after %d seconds\n", err, cnt)
            time.Sleep(cnt * time.Second)
            continue
        } else {
            break
        }
    }
    
    return t.DBEngine.NewOrm()
}

func (t *Driver) TableName(col string) string {
    return t.TablePrefix + col
}

/// Read the object and unmarshal it to v
func (t *Driver) Read(col string, key string, v interface{}) error {
    raw, err := t.ReadRaw(col, key)
    if err != nil {
        return err
    }
    
    json.Unmarshal(raw, &v)
    
    return nil
}

/// Read the raw content of the object.
func (t *Driver) ReadRaw(col string, key string) ([]byte, error) {
    err := t.TryCreateTable(col)
    if err != nil {
        return nil, err
    }
    
    data, err := t.DB().Table(t.TableName(col)).Where("id", key).Fields("j").First()
    if err != nil {
        return nil, err
    }
    
    if data["j"] == nil {
        return nil, errors.New("Document not exists")
    }
    return []byte(data["j"].(string)), nil
}

/// Read the object and unmarshal it to v, but with jsoniter encoder, this may speed up a little.
func (t *Driver) ReadWithJsoniter(col string, key string, v interface{}) error {
    err := t.TryCreateTable(col)
    if err != nil {
        return err
    }
    
    data, err := t.DB().Table(t.TableName(col)).Where("id", key).Fields("j").First()
    if err != nil {
        return err
    }
    
    if data["j"] == nil {
        return errors.New("Document not exists")
    }
    jsoniter.Unmarshal([]byte(data["j"].(string)), &v)
    
    return nil
}

/// Write an object
func (t *Driver) Write(col string, key string, i interface{}) error {
    err := t.TryCreateTable(col)
    if err != nil {
        return err
    }
    
    raw, err := json.MarshalIndent(i, "", "\t")
    if err != nil {
        return err
    }
    
    xxh := xxhash.Sum64(append(raw, []byte(key)...))
    
    /// FIXME: Table name should be escaped.
    _, err = t.DB().Execute(fmt.Sprintf("INSERT INTO `%s` (id, j, xxh) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE j=?, xxh=?", t.TableName(col)), key, raw, xxh, raw, xxh)
    
    if t.AfterUpdateFunc != nil {
        t.AfterUpdateFunc(col, key, i)
    }
    
    return err
}

/// Write an object only if it is altered. (Only do writing when the new data is different with the old data in db)
func (t *Driver) WriteIgnore(col string, key string, i interface{}) error {
    err := t.TryCreateTable(col)
    if err != nil {
        return err
    }
    
    raw, err := json.MarshalIndent(i, "", "\t")
    if err != nil {
        return err
    }
    
    xxh := xxhash.Sum64(append(raw, []byte(key)...))
    
    /// FIXME: Table name should be escaped.
    _, err = t.DB().Execute(fmt.Sprintf("INSERT IGNORE INTO `%s` (id, j, xxh) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE j=?, xxh=?", t.TableName(col)), key, raw, xxh, raw, xxh)
    
    if t.AfterUpdateFunc != nil {
        t.AfterUpdateFunc(col, key, i)
    }
    
    return err
}


/// Get all keys of a collection
func (t *Driver) Keys(col string) ([]string, error) {    
    err := t.TryCreateTable(col)
    if err != nil {
        return nil, err
    }
    
    orm := t.DB()
    orm.Table(t.TableName(col)).Fields("id").Select()
    data, err :=  orm.Get()
    if err != nil {
        return nil, err
    }
    
    keys := make([]string, len(data))
    for k, v := range data {
        keys[k] = v["id"].(string)
        //keys = append(keys, v["id"].(string))
    }
    
    return keys, nil
}

/// Get all keys of a collection with customize SQL WHERE condition.
/// Warning: You have to make sure there is no SQL injection vulnerability in WhereSQL string.
func (t *Driver) KeysWhereSQL(col string, WhereSQL string) ([]string, error) {
    err := t.TryCreateTable(col)
    if err != nil {
        return nil, err
    }
    
    /// FIXME: 
    data, err := t.DB().GetISession().Query(fmt.Sprintf("SELECT id FROM %s WHERE %s", t.TableName(col), WhereSQL))
    if err != nil {
        return nil, err
    }
    if data == nil {
        return []string{}, nil
    }
    
    ret := make([]string, len(data))
    for k, v := range data {
        ret[k] = v["id"].(string)
    }
    
    return ret, nil
}

/// Try to create table for the collection
func (t *Driver) TryCreateTable(col string) error {
    if _, ok := t.ExistsCol.Load(col); ok != true {
        /// FIXME: Table name should be escaped.
        _, err := t.DB().Execute(fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` ( `aid` BIGINT NOT NULL AUTO_INCREMENT, `id` VARCHAR(128) NOT NULL, j LONGTEXT NOT NULL, xxh BIGINT UNSIGNED, create_time INT DEFAULT UNIX_TIMESTAMP(), update_time DATETIME DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP(), PRIMARY KEY (`aid`), UNIQUE (`id`), UNIQUE (`xxh`)) ROW_FORMAT=COMPRESSED", t.TableName(col)))
        if err != nil {
            return err
        } else {
            t.ExistsCol.Store(col, true)
        }
    }
    
    return nil
}

/// Compare if two JSON string is equal in object level.
func DeepEqualRaw(a, b []byte) bool {
    var aj, bj map[string]interface{}
    var err error
    
    err = jsoniter.Unmarshal(a, &aj)
    if err != nil {
        return false
    }
    err = jsoniter.Unmarshal(b, &bj)
    if err != nil {
        return false
    }
    
    return reflect.DeepEqual(aj, bj)
}
