# sqljsondb [![GoDoc](https://godoc.org/github.com/greensea/sqljsondb?status.svg)](http://godoc.org/github.com/greensea/sqljsondb)

a key-value json document DB but use MySQL as storage

## Usage
```
db := sqljsondb.New("user:pass@tcp(127.0.0.1:3306)/dbname", "")


db.Write("people", "1", "Alice")
db.Write("people", "2", "Bob")

var name string
db.Read("people", "2", name)
fmt.Println(name)     /// Bob

fmt.Println(db.Keys("people"))    /// {"1", "2"}


db.WriteIgnore("people", "2", "Bob")   /// Does nothing because document "2" is already "Bob"
db.WriteIgnore("people", "2", "Evil")  /// Document "2" updated to "Evil"
```

For more usage check [doc](http://godoc.org/github.com/greensea/sqljsondb)

