package main

import "C"
import (
	"bufio"
	"bytes"
	"database/sql"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/test_driver"

	// _ "github.com/pingcap/parser/test_driver"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/Jeffail/gabs/v2"
	"github.com/aws/aws-sdk-go/aws/credentials"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
)

//parse functions takes a statement and returns an ast node.
func parse(sql string) (*ast.StmtNode, error) {
	p := parser.New()

	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return nil, err
	}
	return &stmtNodes[0], nil
}

type colX struct {
	colNames   []string
	tableNames []string
}
type fetchPtr struct {
	fetchedPtr ast.ExprNode
}

var scopeVar int = 0
var queryCols []string
var SearchString string = ""
var startAppendingCols bool = false
var ctxvariable = -1

// var ptr ast.ExprNode = &ast.MatchAgainst{}

func (v *colX) Enter(in ast.Node) (ast.Node, bool) {
	// fmt.Printf("%T\n", in)
	// fmt.Printf("%v\n", in)
	scopeVar += 1
	// fmt.Printf("____________\n")
	if va, ok := in.(*ast.MatchAgainst); ok {
		// fmt.Println("****************")
		ctxvariable = int(va.Modifier)
		// fmt.Println(va.Type.EvalType())
		// fmt.Println("****************")
		startAppendingCols = true
	}
	if name, ok := in.(*ast.ColumnName); ok {
		if startAppendingCols {
			// fmt.Printf("SEARCH COLUMN NAME %v\n", name.Name.L)
			queryCols = append(queryCols, name.Name.L)
		}
		v.colNames = append(v.colNames, name.Name.O)
	}
	if name, ok := in.(*test_driver.ValueExpr); ok && startAppendingCols && ctxvariable == 1 {
		// fmt.Printf("VALUE OF EXPRESSION : %v\n", name.GetDatumString())
		SearchString = name.GetDatumString()
	}
	if name, ok := in.(*ast.TableName); ok {
		v.tableNames = append(v.tableNames, name.Name.O)
	}
	// if name, ok := in.(*ast.MatchAgainst); ok {
	// 	for _, val := range name.ColumnNames {
	// 		fmt.Printf("%v\n", val.Name)
	// 	}
	// }
	return in, false
}
func makeInQuery(querystring string) string {
	// domain := "https://vpc-mysql-esoffload-2ae53yabkcyzvp3beuvxkoawsi.us-east-1.es.amazonaws.com" // e.g. https://my-domain.region.es.amazonaws.com
	domain := "https://vpc-mysqlftsofld60-fvkktyi4k4kqqiyes52iu4v5z4.us-east-1.es.amazonaws.com"
	index := "testing.article"
	primary_col := "article_id"
	endpoint := domain + "/" + index + "/" + "_search"
	region := "us-east-1"
	service := "es"
	colnameofquery := "article_content"
	var str bytes.Buffer
	str.WriteString(fmt.Sprintf("SELECT * from XYZ WHERE %v IN (", primary_col))
	checkthis := querystring
	// checkthis = "Ever thousand recognize"
	// fmt.Println(checkthis)
	json1 := fmt.Sprintf(`{
		"_source":["article_id"],
		"query":{
			"match_phrase": {
				"%v":%v
			}
		},
		"size":9999
	}`, colnameofquery, checkthis)
	body := strings.NewReader(json1)
	credentials := credentials.NewEnvCredentials()
	signer := v4.NewSigner(credentials)
	client := &http.Client{}
	req, err := http.NewRequest(http.MethodGet, endpoint, body)
	if err != nil {
		log.Fatalln(err)
	}
	req.Header.Add("Content-Type", "application/json")
	signer.Sign(req, body, service, region, time.Now())
	resp, err := client.Do(req)
	if err != nil {
		fmt.Print(err)
	}

	bdy, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalln(err)
	}
	jsonParsed, err := gabs.ParseJSON(bdy)
	if err != nil {
		fmt.Println(err)
	}
	// fmt.Println(jsonParsed)
	var idlist []float64
	for _, child1 := range jsonParsed.S("hits", "hits").Children() {
		for _, child2 := range child1.S("_source").Children() {
			idlist = append(idlist, child2.Data().(float64))
		}
	}
	if len(idlist) == 0 {
		str.WriteString("-1")
	}
	count := 0
	for i, val := range idlist {
		count += 1
		str.WriteString(fmt.Sprint(val))
		if i < len(idlist)-1 {
			str.WriteString(",")
		}
	}
	// fmt.Println(count)
	str.WriteString(")")
	return str.String()
}
func (v *colX) Leave(in ast.Node) (ast.Node, bool) {
	// fmt.Printf("scope : %T\n", in)
	// fmt.Printf("scope : %v\n", in)
	if _, ok := in.(*ast.MatchAgainst); ok && ctxvariable == 1 && SearchString[0] == '"' {
		queryString := makeInQuery(SearchString)
		tempAstNode, err := parse(queryString)
		if err != nil {
			fmt.Printf("parse error: %v\n", err.Error())
			return in, true
		}
		queryCols = nil
		SearchString = ""
		startAppendingCols = false
		ctxvariable = -1
		u := &fetchPtr{}
		u.fetchedPtr = &ast.PatternInExpr{}
		extractIDinPtr(u, tempAstNode)
		in = u.fetchedPtr

	}
	// if in1, ok := in.(*ast.MatchAgainst); ok {
	// 	ptr = in1
	// }
	// if _, ok := in.(*ast.PatternInExpr); ok {
	// 	in = ptr
	// }
	return in, true
}

func extract_cols(rootNode *ast.StmtNode) {
	v := &colX{}
	(*rootNode).Accept(v)
	return
}
func extractIDinPtr(u *fetchPtr, rootNode *ast.StmtNode) {
	(*rootNode).Accept(u)
}
func (v *fetchPtr) Enter(in ast.Node) (ast.Node, bool) {
	if name, ok := in.(*ast.PatternInExpr); ok {
		v.fetchedPtr = name
		return in, true
	}
	return in, false
}
func (v *fetchPtr) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}
func wf(sql string) string {
	astNode, _ := parse(sql)
	extract_cols(astNode)
	var sb strings.Builder
	val := *astNode
	val.Restore(format.NewRestoreCtx(265, &sb))
	return sb.String()
}
func checkIfOneWord(check string) bool {
	for _, w := range check {
		if w == ' ' {
			return false
		}
	}
	return true
}

//export WrapperFunc
func WrapperFunc(sqlquery *C.char) *C.char {
	sql := C.GoString((*C.char)(sqlquery))
	astNode, _ := parse(sql)
	extract_cols(astNode)
	var sb strings.Builder
	val := *astNode
	val.Restore(format.NewRestoreCtx(265, &sb))
	return C.CString(sb.String())
}

type User struct {
	Name string `json:"article_title"`
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}
func main() {
	db, err := sql.Open("mysql", "admin:jtues2022@tcp(jtu-esoffload-mysql-provisoned-instance-1.cbzs7kxmytip.us-east-1.rds.amazonaws.com:3306)/testing")

	if err != nil {
		panic(err.Error())
	}
	defer db.Close()
	file, err := os.Open("phrasematch2")
	if err != nil {
		log.Fatalf("failed to open")

	}
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	var text []string
	for scanner.Scan() {
		text = append(text, scanner.Text())
	}
	file.Close()
	f, err := os.Create("compfile6")
	check(err)
	w := bufio.NewWriter(f)
	for _, each_ln := range text {
		inp_string := fmt.Sprintf(`select article_title from article where MATCH(article_content) AGAINST('"%v"' IN BOOLEAN MODE)`, each_ln)
		// inp_string := fmt.Sprintf(`select article_title from article where MATCH(article_content) AGAINST('"Lot end individual four"' IN BOOLEAN MODE)`)
		// resstr := wf(inp_string)
		start := time.Now()
		results, err := db.Query(inp_string)
		if err != nil {
			panic(err.Error())
		}
		duration1 := time.Since(start)
		_, err = fmt.Fprintf(w, "%v,", duration1)
		check(err)
		cnt1 := 0
		for results.Next() {
			// var user User
			// err = results.Scan(&user.Name)
			// if err != nil {
			// 	panic(err.Error())
			// }
			// fmt.Println(user.Name)
			cnt1 += 1
		}
		start = time.Now()
		recquery := wf(inp_string)
		duration2 := time.Since(start)
		_, err = fmt.Fprintf(w, "%v,", duration2)
		check(err)
		results, err = db.Query(recquery)
		if err != nil {
			panic(err.Error())
		}
		duration2 = time.Since(start)
		_, err = fmt.Fprintf(w, "%v,", duration2)
		check(err)
		_, err = fmt.Fprintf(w, "%v,", (duration1-duration2)*100/(duration1))
		check(err)
		cnt2 := 0
		for results.Next() {
			// var user User
			// err = results.Scan(&user.Name)
			// if err != nil {
			// 	panic(err.Error())
			// }
			// fmt.Println(user.Name)
			cnt2 += 1
		}
		_, err = fmt.Fprintf(w, "%v,", cnt1)
		check(err)
		_, err = fmt.Fprintf(w, "%v\n", cnt2)
		check(err)
		results = results
		w.Flush()
	}
	f.Close()
}
