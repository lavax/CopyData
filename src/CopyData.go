package main

import (
	"database/sql"
	"log"

	_ "github.com/go-sql-driver/mysql"

	_ "github.com/alexbrainman/odbc"
)

/*
	拷贝数据程序

	2017/12
	Eric
	bigfrog7@sohu.com
*/

var mssqlDriver = "odbc"
var mssqlConnStr = "driver={sql server};server=192.168.96.129;port=1401;uid=sa;pwd=Sinolife@2018;database=testdb"

var mysqlDriver = "mysql"
var mysqlConnStr = "sa:123456@tcp(192.168.96.129:3306)/hxgkztb?charset=utf8"

//供应商表
type gys_dep struct {
	id             sql.NullInt64
	name           sql.NullString
	zhuying        sql.NullString
	org_code       sql.NullString
	faren          sql.NullString
	faren_code     sql.NullString
	reg_type       sql.NullInt64
	contactor      sql.NullString
	tel            sql.NullString
	mobile         sql.NullString
	fax            sql.NullString
	contactor2     sql.NullString
	tel2           sql.NullString
	mobile2        sql.NullString
	fax2           sql.NullString
	category       sql.NullString
	zzdj           sql.NullString
	status         sql.NullInt64
	audit_reason   sql.NullString
	remark         sql.NullString
	created_at_str sql.NullString
	updated_at_str sql.NullString
}

//供应商_材料库
type gys_category struct {
	id              sql.NullInt64
	gys_id          sql.NullInt64
	prd_category_id sql.NullInt64
}

//材料库
type prd_category struct {
	id             sql.NullInt64
	name           sql.NullString
	code           sql.NullString
	xinghao        sql.NullString
	unit           sql.NullString
	parent_id      sql.NullInt64
	is_leaf        sql.NullInt64
	is_business    sql.NullInt64
	lft            sql.NullInt64
	rgt            sql.NullInt64
	created_at_str sql.NullString
	updated_at_str sql.NullString
	created_at     sql.NullString
	updated_at     sql.NullString
}

//供应商资质证明
type gys_cert struct {
	id                sql.NullInt64
	gys_dep_id        sql.NullInt64
	file_tag          sql.NullString
	original_filename sql.NullString
	filesize          sql.NullInt64
	disk_filename     sql.NullString
	disk_path         sql.NullString
	content_type      sql.NullString
	user_id           sql.NullInt64
	user_name         sql.NullString
	user_type         sql.NullString
	guid              sql.NullString
	created_at_str    sql.NullString
	updated_at_str    sql.NullString
	created_at        sql.NullString
	updated_at        sql.NullString
	use_for           sql.NullInt64
}

//供应商用户
type gys_user struct {
	id             sql.NullInt64
	gys_dep_id     sql.NullInt64
	login          sql.NullString
	password       sql.NullString
	real_name      sql.NullString
	sex            sql.NullString
	status         sql.NullInt64
	register_from  sql.NullInt64
	tel            sql.NullString
	mobile         sql.NullString
	email          sql.NullString
	created_at_str sql.NullString
	updated_at_str sql.NullString
	created_at     sql.NullString
	updated_at     sql.NullString
}

func init() {
	log.SetPrefix("[DEBUG]")
}

func main() {
	//用外部机制定时执行，程序只拷贝数据
	copyData()

}

/*
	拷贝数据
*/
func copyData() {
	//查询源表，将数据载入内存
	gysDeps := getGysDeps()
	gysCategorys := getGysCategorys()
	prdCategorys := getPrdCategorys()
	gysCerts := getGysCerts()
	gysUsers := getGysUsers()

	//TODO:Save
	saveGysDeps(gysDeps)
	saveGysCategorys(gysCategorys)
	savePrdCategorys(prdCategorys)
	saveGysCerts(gysCerts)
	saveGysUsers(gysUsers)
}

//获取供应商表
func getGysDeps() []gys_dep {
	log.Println("start")
	var selectSql = "SELECT id,name,zhuying,org_code,faren,faren_code,reg_type,contactor,tel,mobile,fax,contactor2,tel2,mobile2,fax2,category,zzdj,status,audit_reason,remark,created_at,updated_at FROM hxgkztb.gys_dep"
	log.Println(selectSql)
	var gysDeps = []gys_dep{}

	conn, err := sql.Open(mysqlDriver, mysqlConnStr)
	if err != nil {
		log.Println("Connecting Error", mysqlDriver, mysqlConnStr, err)
		return gysDeps
	}
	defer conn.Close()
	stmt, err := conn.Prepare(selectSql)
	if err != nil {
		log.Println("stmt.Prepare Error", mysqlDriver, mysqlConnStr, err)
		return gysDeps
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil {
		log.Println("stmt.Query Error", mysqlDriver, mysqlConnStr, err)
		return gysDeps
	}
	defer rows.Close()
	for rows.Next() {
		var aRow gys_dep
		err := rows.Scan(&aRow.id, &aRow.name, &aRow.zhuying, &aRow.org_code, &aRow.faren, &aRow.faren_code, &aRow.reg_type, &aRow.contactor, &aRow.tel, &aRow.mobile, &aRow.fax, &aRow.contactor2, &aRow.tel2, &aRow.mobile2, &aRow.fax2, &aRow.category, &aRow.zzdj, &aRow.status, &aRow.audit_reason, &aRow.remark, &aRow.created_at_str, &aRow.updated_at_str)
		// created_at, _ := time.Parse("2006-01-02 15:04:05", created_at_str)
		// updated_at, _ := time.Parse("2006-01-02 15:04:05", updated_at_str)
		if err == nil {
			gysDeps = append(gysDeps, aRow)
		} else {
			panic(err)
		}
	}
	log.Println(len(gysDeps))
	log.Println("end")
	return gysDeps
}

//供应商_材料库
func getGysCategorys() []gys_category {
	log.Println("start")
	var selectSql = "SELECT gys_category.id, gys_category.gys_id, gys_category.prd_category_id FROM hxgkztb.gys_category"
	var gysCategorys = []gys_category{}

	conn, err := sql.Open(mysqlDriver, mysqlConnStr)
	if err != nil {
		log.Println("Connecting Error", mysqlDriver, mysqlConnStr, err)
		return gysCategorys
	}
	defer conn.Close()
	stmt, err := conn.Prepare(selectSql)
	if err != nil {
		log.Println("stmt.Prepare Error", mysqlDriver, mysqlConnStr, err)
		return gysCategorys
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil {
		log.Println("stmt.Query Error", mysqlDriver, mysqlConnStr, err)
		return gysCategorys
	}
	defer rows.Close()
	for rows.Next() {
		var aRow gys_category
		if err := rows.Scan(&aRow.id, &aRow.gys_id, &aRow.prd_category_id); err == nil {
			gysCategorys = append(gysCategorys, aRow)
		}
	}
	log.Println(len(gysCategorys))
	log.Println("end")
	return gysCategorys
}

//获取材料库
func getPrdCategorys() []prd_category {
	log.Println("start")
	var selectSql = "SELECT id,    name,    code,    xinghao,    unit,    parent_id,    is_leaf,    is_business,    lft,    rgt,date_format(created_at, '%Y-%m-%d %H:%i:%s'),date_format(updated_at, '%Y-%m-%d %H:%i:%s') FROM hxgkztb.prd_category"
	var prdCategorys = []prd_category{}

	conn, err := sql.Open(mysqlDriver, mysqlConnStr)
	if err != nil {
		log.Println("Connecting Error", mysqlDriver, mysqlConnStr, err)
		return prdCategorys
	}
	defer conn.Close()
	stmt, err := conn.Prepare(selectSql)
	if err != nil {
		log.Println("stmt.Prepare Error", mysqlDriver, mysqlConnStr, err)
		return prdCategorys
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil {
		log.Println("stmt.Query Error", mysqlDriver, mysqlConnStr, err)
		return prdCategorys
	}
	defer rows.Close()
	for rows.Next() {
		var aRow prd_category
		err := rows.Scan(&aRow.id, &aRow.name, &aRow.code, &aRow.xinghao, &aRow.unit, &aRow.parent_id, &aRow.is_leaf, &aRow.is_business, &aRow.lft, &aRow.rgt, &aRow.created_at_str, &aRow.updated_at_str)
		if err == nil {
			prdCategorys = append(prdCategorys, aRow)
		} else {
			panic(err)
		}
	}
	log.Println(len(prdCategorys))
	log.Println("end")
	return prdCategorys
}

//获取供应商资质证明
func getGysCerts() []gys_cert {
	log.Println("start")
	var selectSql = "SELECT id,    gys_dep_id,    file_tag,    original_filename,    filesize,    disk_filename,    disk_path,    content_type,    user_id,    user_name,    user_type,    guid,use_for,    date_format(created_at, '%Y-%m-%d %H:%i:%s'),date_format(updated_at, '%Y-%m-%d %H:%i:%s') FROM hxgkztb.gys_cert"
	var gysCerts = []gys_cert{}

	conn, err := sql.Open(mysqlDriver, mysqlConnStr)
	if err != nil {
		log.Println("Connecting Error", mysqlDriver, mysqlConnStr, err)
		return gysCerts
	}
	defer conn.Close()
	stmt, err := conn.Prepare(selectSql)
	if err != nil {
		log.Println("stmt.Prepare Error", mysqlDriver, mysqlConnStr, err)
		return gysCerts
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil {
		log.Println("stmt.Query Error", mysqlDriver, mysqlConnStr, err)
		return gysCerts
	}
	defer rows.Close()
	for rows.Next() {
		var aRow gys_cert
		err := rows.Scan(&aRow.id, &aRow.gys_dep_id, &aRow.file_tag, &aRow.original_filename, &aRow.filesize, &aRow.disk_filename, &aRow.disk_path, &aRow.content_type, &aRow.user_id, &aRow.user_name, &aRow.guid, &aRow.user_type, &aRow.use_for, &aRow.created_at_str, &aRow.updated_at_str)
		if err == nil {
			gysCerts = append(gysCerts, aRow)
		} else {
			panic(err)
		}
	}
	log.Println(len(gysCerts))
	log.Println("end")
	return gysCerts
}

//获取供应商用户
func getGysUsers() []gys_user {
	log.Println("start")
	var selectSql = "SELECT id,    gys_dep_id,    login,    password,    real_name,    sex,    status,    register_from,    tel,    mobile,    email,date_format(created_at, '%Y-%m-%d %H:%i:%s'),date_format(updated_at, '%Y-%m-%d %H:%i:%s') FROM hxgkztb.gys_user"
	var gysUsers = []gys_user{}

	conn, err := sql.Open(mysqlDriver, mysqlConnStr)
	if err != nil {
		log.Println("Connecting Error", mysqlDriver, mysqlConnStr, err)
		return gysUsers
	}
	defer conn.Close()
	stmt, err := conn.Prepare(selectSql)
	if err != nil {
		log.Println("stmt.Prepare Error", mysqlDriver, mysqlConnStr, err)
		return gysUsers
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil {
		log.Println("stmt.Query Error", mysqlDriver, mysqlConnStr, err)
		return gysUsers
	}
	defer rows.Close()
	for rows.Next() {
		var aRow gys_user
		err := rows.Scan(&aRow.id, &aRow.gys_dep_id, &aRow.login, &aRow.password, &aRow.real_name, &aRow.sex, &aRow.status, &aRow.register_from, &aRow.tel, &aRow.mobile, &aRow.email, &aRow.created_at_str, &aRow.updated_at_str)
		if err == nil {
			gysUsers = append(gysUsers, aRow)
		} else {
			panic(err)
		}
	}
	log.Println(len(gysUsers))
	log.Println("end")
	return gysUsers
}

//保存供应商表
func saveGysDeps(gysDeps []gys_dep) {
	for k, gysDep := range gysDeps {
		log.Print("gysDep", k, gysDep.id, gysDep.name, gysDep.org_code, gysDep.created_at_str)
	}
}

//保存供应商_材料库
func saveGysCategorys(gysCategorys []gys_category) {
	for k, gysCategory := range gysCategorys {
		log.Print(k, gysCategory.id, gysCategory.gys_id, gysCategory.prd_category_id)
	}
	/*



		log.Println("start")
		var insertSql = "INSERT INTO gys_category ('id', 'gys_id', 'prd_category_id') VALUES (?,?,?)"

		conn, err := sql.Open(mysqlDriver, mysqlConnStr)
		if err != nil {
			log.Println("Connecting Error")
			return
		}
		defer conn.Close()
		stmt, err := conn.Prepare(insertSql)
		if err != nil {
			log.Println("Query Error", err)
			return
		}
		defer stmt.Close()
		for k, gysCategory := range gysCategorys {
			log.Println(k)
			log.Println(gysCategory.id)
			rs, err := stmt.Exec(gysCategory.id, gysCategory.gys_id, gysCategory.prd_category_id)
			log.Println(rs)
			if err != nil {
				log.Println("Query Error", err)
				return
			}
		}
		log.Println("end")
	*/
}

//保存材料库
func savePrdCategorys(prdCategorys []prd_category) {
	for k, prdCategory := range prdCategorys {
		log.Print("prdCategory", k, prdCategory.id, prdCategory.name, prdCategory.code, prdCategory.created_at_str.String)
	}
}

//保存供应商资质证明
func saveGysCerts(gysCerts []gys_cert) {
	for k, gysCerts := range gysCerts {
		log.Print("gysCert", k, gysCerts.id, gysCerts.gys_dep_id, gysCerts.file_tag, gysCerts.created_at_str.String)
	}
}

//保存供应商用户
func saveGysUsers(gysUsers []gys_user) {
	for k, gysUser := range gysUsers {
		log.Print("gysUser", k, gysUser.id, gysUser.gys_dep_id, gysUser.login, gysUser.created_at_str.String)
	}
}
