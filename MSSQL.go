package main

import (
    "strconv"
    "fmt"
    "flag"
    "os"
    "github.com/tidwall/gjson"
    "io/ioutil"
    "strings"
    "compress/gzip"
    "path/filepath"
//    "database/sql"
//    "net/url"
//    "github.com/denisenkom/go-mssqldb"
)

var (
    debug       = flag.Bool("debug", true, "enable debugging")
    password    = flag.String("password", "test", "db password")
    port *int   = flag.Int("port", 1433, "db port")
    server      = flag.String("server", "localhost", "db server")
    user        = flag.String("user", "osmtest", "db user")
    database    = flag.String("database", "canvastest", "db name")
)

//Read Schema
func readSchema(fileName string) []byte{
    raw, err := ioutil.ReadFile(fileName)
    if err != nil {
        fmt.Println(err.Error())
        os.Exit(1)
    }

    return raw
}

//Read schema and return the table name + column names. 
func getHeader(fileName string, body []byte) (string, gjson.Result) {

    dlPath := ("../Canvas-go/Downloads/")
    stringSlice := strings.Split(fileName, "-")
    tableName := stringSlice[0]
    if _, err := os.Stat(dlPath + fileName); os.IsNotExist(err){
        fmt.Println("Error with: " + tableName)
    }

    //value := gjson.GetBytes(body, "schema."+tableName)
    if !gjson.GetBytes(body, "schema."+tableName).Exists(){
        stringSlice2 := strings.Split(fileName, "_")
        tableName = stringSlice2[0]
    }

    result := gjson.GetBytes(body, "schema." + tableName + ".columns.#.name")

    return tableName, result
}

//write script for processing create columns and column type
func createSQL(schema []byte, tableName string){

    f, err := os.OpenFile("createTables.sql", os.O_APPEND | os.O_WRONLY | os.O_CREATE, 0644)

    if err != nil {
        panic(err)
    }
    
    schPath := "schema." + tableName + ".columns"
    colCount := int(gjson.GetBytes(schema, schPath + ".#").Int())

    createQuery := "CREATE TABLE " + tableName + "\n("

    var colName string
    var colType string
    for i:=0; i<colCount; i++ {
        colName = gjson.GetBytes(schema, schPath + "." + strconv.Itoa(i) + ".name").String()
        colType = gjson.GetBytes(schema, schPath + "." + strconv.Itoa(i) + ".type").String()
        createQuery = createQuery + "\n\t" + colName + "\t" + colType 
        if i<colCount-1 {
            createQuery = createQuery + ","
        }
    }
    
    createQuery = createQuery + "\n);\n"

    _, err = f.WriteString(createQuery)
    
    if err!= nil{
        panic(err)
    }
    
    f.Close()
    //fmt.Println(createQuery)
}

//Create new tab delimited file with headers.
func newTable(tableName string, fileName string, headers gjson.Result){

    var header string
    for _, name := range headers.Array(){
        if len(header) > 0 {
            header = header + "\t" + name.String()
        } else {
            header = name.String()
        }
    }

    header = header + "\n"
    dirPath := ("./Parsed/")
    f, _ := os.Create(dirPath + tableName + ".gz")

    filePath := "../Canvas-go/Downloads/" + fileName

    body, _ := readGZFile(filePath)

    d2 := append([]byte(header), body...)

    w, _ := gzip.NewWriterLevel(f, gzip.BestCompression)
    w.Write(d2)
    w.Close()
}

func prepFiles() {

    schema := readSchema("schema.json")

    dlPath := ("../Canvas-go/Downloads/")
    var fileName string

    //Get files in directory
    fileList := []string{}
    err := filepath.Walk(dlPath, func(path string, f os.FileInfo, err error) error {
        if filepath.Ext(path) == ".gz" {
            fileList = append(fileList, f.Name())
        }
        return nil
    })

    if err != nil{
        fmt.Printf("Walk Error %v\n", err)
    }

    for _, file := range fileList {
        fileName = file
        tableName, headers := getHeader(fileName, schema)
        newTable(tableName, fileName, headers)
    }

        createSQL(schema, "account")
}

//Read zip file, return contents
func readGZFile(filePath string) ([]byte, error){
    fi, err := os.Open(filePath)
    if err !=nil {
        return nil, err
    }
    defer fi.Close()

    fz, err := gzip.NewReader(fi)
    if err != nil {
        return nil, err
    }
    defer fz.Close()

    s, err := ioutil.ReadAll(fz)
    if err != nil {
        return nil, err
    }

    return s, nil
}

func main() {
    flag.Parse()

    if *debug {
        fmt.Printf(" password:%s\n", *password)
        fmt.Printf(" port:%d\n", *port)
        fmt.Printf(" server:%s\n", *server)
        fmt.Printf(" user:%s\n", *user)
        fmt.Printf(" database:%s\n", *database)
    }

    //COMBINE processes
    prepFiles()
}
