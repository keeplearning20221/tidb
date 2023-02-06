// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/sqlexec"

	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestCreateShowDropProcedure(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	// no database
	tk.MustGetErrCode("create procedure sp_test() begin select@a; end;", 1046)
	tk.MustGetErrCode("create procedure test2.sp_test() begin select@a; end;", 1049)
	tk.MustExec("create procedure test.sp_test() begin select@a; end;")
	tk.MustQuery("show create procedure test.sp_test").Check(testkit.Rows("sp_test ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION " +
		" CREATE PROCEDURE `sp_test`() begin select@a; end utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustExec("use test")
	tk.MustGetErrCode("create procedure sp_test() begin select@a; end;", 1304)
	tk.MustExec("create procedure if not exists sp_test() begin select@b; end;")
	tk.MustQuery("show create procedure test.sp_test").Check(testkit.Rows("sp_test ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION " +
		" CREATE PROCEDURE `sp_test`() begin select@a; end utf8mb4 utf8mb4_bin utf8mb4_bin"))
	// in/out/inout
	tk.MustExec("create procedure if not exists sp_test1(id int) begin select@b; end;")
	tk.MustQuery("show create procedure sp_test1").Check(testkit.Rows("sp_test1 ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION " +
		" CREATE PROCEDURE `sp_test1`(id int) begin select@b; end utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustExec("create procedure if not exists sp_test2(in id int) begin select@b; end;")
	tk.MustQuery("show create procedure sp_test2").Check(testkit.Rows("sp_test2 ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION " +
		" CREATE PROCEDURE `sp_test2`(in id int) begin select@b; end utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustExec("create procedure if not exists sp_test3(out id int) begin select@b; end;")
	tk.MustQuery("show create procedure sp_test3").Check(testkit.Rows("sp_test3 ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION " +
		" CREATE PROCEDURE `sp_test3`(out id int) begin select@b; end utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustExec("create procedure if not exists sp_test4(inout id int) begin select@b; end;")
	tk.MustQuery("show create procedure sp_test4").Check(testkit.Rows("sp_test4 ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION " +
		" CREATE PROCEDURE `sp_test4`(inout id int) begin select@b; end utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustExec("create procedure if not exists sp_test5(id int,in id1 int,out id2 varchar(100),inout id3 int) begin select@b; end;")
	tk.MustQuery("show create procedure sp_test5").Check(testkit.Rows("sp_test5 ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION " +
		" CREATE PROCEDURE `sp_test5`(id int,in id1 int,out id2 varchar(100),inout id3 int) begin select@b; end utf8mb4 utf8mb4_bin utf8mb4_bin"))
	// Duplicate input parameter name.
	tk.MustGetErrCode("create procedure sp_test6(in id int, out id int) begin select @a; end;", 1330)
	// parameter does not exist.
	tk.MustGetErrCode("create procedure sp_test6() begin set a = 1; end;", 1193)
	// Variables should be applied before sql.
	tk.MustGetErrCode("create procedure sp_test6() begin set @a = 1;declare s varchar(100); end;", 1064)
	// sp variables can only be declared inside.
	tk.MustGetErrCode("create procedure sp_test6() begin set @a = 1; end;declare s varchar(100);", 1064)
	// drop procedure
	tk.MustExec("drop procedure sp_test1")
	tk.MustExec("drop procedure sp_test2")
	tk.MustExec("drop procedure sp_test3")
	tk.MustExec("drop procedure sp_test4")
	tk.MustExec("drop procedure sp_test5")
	err := tk.QueryToErr("show create procedure sp_test1")
	require.EqualError(t, err, "[executor:1305]PROCEDURE sp_test1 does not exist")

	testcases := []string{"create procedure proc_1() begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END;",
		"create procedure if not exists proc_2() begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END;",
		"create procedure if not exists proc_3(in id int,inout id2 int,out id3 int) begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END;",
		"create procedure proc_4(in id bigint,in id2 varchar(100),in id3 decimal(30,2)) begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END;",
		"create procedure proc_5(in id double,in id2 float,out id3 char(10),in id4 binary) begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END;",
		"create procedure proc_6(in id VARBINARY(30),in id2 BLOB,out id3 TEXT,in id4 ENUM('1','2')) begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END;",
		"create procedure proc_7(in id SET('1','2')) begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END;",
		"create procedure proc_8(in id SET('1','2')) begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select a.id,a.username,a.password,a.age,a.sex from user a where a.id > 10 and a.id < 50;" +
			"select us.subject,count(us.user_id),sum(us.score),avg(us.score),max(us.score),min(us.score) from user_score us where us.score > 90 group by us.subject;END;",
		"create procedure proc_9(in id SET('1','2')) begin select *,rank() over (partition by subject order by score desc) as ranking from user_score;select *,rank() over (partition by subject order by score desc) as ranking from user_score;end",
		"create procedure proc_10(in id SET('1','2')) begin select us.*,sum(us.score) over (order by us.id) as current_sum," +
			"avg(us.score) over (order by us.id) as current_avg,count(us.score) over (order by us.id) as current_count,max(us.score) over (order by us.id) as current_max,min(us.score) over (order by us.id) as current_min from user_score us;" +
			"select us.*,sum(us.score) over (order by us.id) as current_sum, avg(us.score) over (order by us.id) as current_avg,count(us.score) over (order by us.id) as current_count,max(us.score) over (order by us.id) as current_max," +
			"min(us.score) over (order by us.id) as current_min,u.username ,ua.address,CONCAT(u.username, \"-\" ,ua.address) as userinfo from user_score us left join user u on u.id = us.user_id left join user_address ua on ua.id = us.user_id; end;",
		"create procedure proc_11() begin SELECT DISTINCT us.user_id,u.username ,ua.address,CONCAT(u.username, \"-\" ,ua.address) as userinfo, sum(us.score) from user_score us left join user u on u.id = us.user_id" +
			"left join user_address ua on ua.id = us.user_id group by us.user_id,u.username;" +
			"select a.subject,a.id,a.user_id,u.username, a.score,a.rownum from (select id,user_id,subject,score,row_number() over (order by score desc) as rownum from user_score) as a left join user u on a.user_id = u.id" +
			"inner join user_score as b on a.id=b.id where a.rownum<=10 order by a.rownum ;" +
			"select a.subject,a.id,a.score,a.rownum from (" +
			"select id,subject,score,row_number() over (partition by subject order by score desc) as rownum from user_score) as a inner join user_score as b on a.id=b.id where a.rownum<=10 order by a.subject ;" +
			"select *,u.username,ua.address,CONCAT(u.username, \"-\" ,ua.address) as userinfo,avg(us.score) over (order by us.id rows 2 preceding) as current_avg,sum(score) over (order by us.id rows 2 preceding) as current_sum from user_score us" +
			" left join user u on u.id = us.user_id left join user_address ua on ua.id = us.user_id;" +
			"select a.id,a.username,a.password,a.age,a.sex from user a where a.id in (select user_id from user_score where score > 90);    end;",
		"create procedure proc_12() begin select us.user_id,u.username,us.subject,us.score from user_score us left join user u on u.id = us.user_id where us.score > 90 group by us.user_id,us.subject,us.score;" +
			"select us.user_id,u.username,us.subject,us.score from user_score us join user u on u.id = us.user_id where us.score > 90 group by us.user_id,us.subject,us.score;" +
			"select a.id,a.username,a.password,a.age,a.sex,ad.address,CONCAT(a.username, \"-\" ,ad.address) as userinfo from user a left join user_address ad on a.id = ad.user_id where a.id > 10 and a.id < 50;" +
			"select a.id,a.username,a.password,a.age,a.sex,ad.score from user a right join user_score ad on a.id = ad.user_id where a.id > 10 and a.id < 50;" +
			"select a.id,a.username,a.password,a.age,a.sex,ad.score from user a left join user_score ad on a.id = ad.user_id where a.id in (select user_id from user_score where score > 90 and score < 99 ) " +
			"union select a.id,a.username,a.password,a.age,a.sex,ad.score from user a left join user_score ad on a.id = ad.user_id where a.id in (select user_id from user_score where score > 30 and score < 70 ); end;",
		//     block recursion
		"create procedure proc_13() begin select @a; begin select @b; end; end",
		"create procedure proc_14() begin select @a; insert into t2 select * from t1; begin select @b;update t2 set id = 1; end; end",
		// test declared variable type
		"create procedure proc_15() begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881); select @a; insert into t2 select * from t1; begin declare s varchar(100); select @b;update t2 set id = 1; end; end",
		"create procedure proc_16() begin declare s varchar(100) ; declare s int; declare s bigint;declare s float;declare s double;select @a; insert into t2 select * from t1; begin declare s varchar(100); select @b;update t2 set id = 1; end; end",
		"create procedure proc_17() begin declare s char(100) ; declare s blob; declare s text;declare s DECIMAL(30,2);declare s datetime;select @a; insert into t2 select * from t1; begin declare s varchar(100); select @b;update t2 set id = 1; end; end",
		// test default
		"create procedure proc_18() begin declare s varchar(100) default s; declare s int default 1; declare s bigint default @a;select @a; insert into t2 select * from t1; begin declare s varchar(100); select @b;update t2 set id = 1; end; end",
		// test insert select
		"create procedure proc_19() begin select @a; insert into t2 select * from t1; begin declare s varchar(100);begin declare s varchar(100);begin declare s varchar(100); select @b;update t2 set id = 1; end; select @b;update t2 set id = 1; end; select @b;update t2 set id = 1; end; end",
	}
	res := []string{
		" CREATE PROCEDURE `proc_1`() begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END",
		" CREATE PROCEDURE `proc_2`() begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END",
		" CREATE PROCEDURE `proc_3`(in id int,inout id2 int,out id3 int) begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END",
		" CREATE PROCEDURE `proc_4`(in id bigint,in id2 varchar(100),in id3 decimal(30,2)) begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END",
		" CREATE PROCEDURE `proc_5`(in id double,in id2 float,out id3 char(10),in id4 binary) begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END",
		" CREATE PROCEDURE `proc_6`(in id VARBINARY(30),in id2 BLOB,out id3 TEXT,in id4 ENUM('1','2')) begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END",
		" CREATE PROCEDURE `proc_7`(in id SET('1','2')) begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END",
		" CREATE PROCEDURE `proc_8`(in id SET('1','2')) begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select a.id,a.username,a.password,a.age,a.sex from user a where a.id > 10 and a.id < 50;" +
			"select us.subject,count(us.user_id),sum(us.score),avg(us.score),max(us.score),min(us.score) from user_score us where us.score > 90 group by us.subject;END",
		" CREATE PROCEDURE `proc_9`(in id SET('1','2')) begin select *,rank() over (partition by subject order by score desc) as ranking from user_score;select *,rank() over (partition by subject order by score desc) as ranking from user_score;end",
		" CREATE PROCEDURE `proc_10`(in id SET('1','2')) begin select us.*,sum(us.score) over (order by us.id) as current_sum," +
			"avg(us.score) over (order by us.id) as current_avg,count(us.score) over (order by us.id) as current_count,max(us.score) over (order by us.id) as current_max,min(us.score) over (order by us.id) as current_min from user_score us;" +
			"select us.*,sum(us.score) over (order by us.id) as current_sum, avg(us.score) over (order by us.id) as current_avg,count(us.score) over (order by us.id) as current_count,max(us.score) over (order by us.id) as current_max," +
			"min(us.score) over (order by us.id) as current_min,u.username ,ua.address,CONCAT(u.username, \"-\" ,ua.address) as userinfo from user_score us left join user u on u.id = us.user_id left join user_address ua on ua.id = us.user_id; end",
		" CREATE PROCEDURE `proc_11`() begin SELECT DISTINCT us.user_id,u.username ,ua.address,CONCAT(u.username, \"-\" ,ua.address) as userinfo, sum(us.score) from user_score us left join user u on u.id = us.user_id" +
			"left join user_address ua on ua.id = us.user_id group by us.user_id,u.username;" +
			"select a.subject,a.id,a.user_id,u.username, a.score,a.rownum from (select id,user_id,subject,score,row_number() over (order by score desc) as rownum from user_score) as a left join user u on a.user_id = u.id" +
			"inner join user_score as b on a.id=b.id where a.rownum<=10 order by a.rownum ;" +
			"select a.subject,a.id,a.score,a.rownum from (" +
			"select id,subject,score,row_number() over (partition by subject order by score desc) as rownum from user_score) as a inner join user_score as b on a.id=b.id where a.rownum<=10 order by a.subject ;" +
			"select *,u.username,ua.address,CONCAT(u.username, \"-\" ,ua.address) as userinfo,avg(us.score) over (order by us.id rows 2 preceding) as current_avg,sum(score) over (order by us.id rows 2 preceding) as current_sum from user_score us" +
			" left join user u on u.id = us.user_id left join user_address ua on ua.id = us.user_id;" +
			"select a.id,a.username,a.password,a.age,a.sex from user a where a.id in (select user_id from user_score where score > 90);    end",
		" CREATE PROCEDURE `proc_12`() begin select us.user_id,u.username,us.subject,us.score from user_score us left join user u on u.id = us.user_id where us.score > 90 group by us.user_id,us.subject,us.score;" +
			"select us.user_id,u.username,us.subject,us.score from user_score us join user u on u.id = us.user_id where us.score > 90 group by us.user_id,us.subject,us.score;" +
			"select a.id,a.username,a.password,a.age,a.sex,ad.address,CONCAT(a.username, \"-\" ,ad.address) as userinfo from user a left join user_address ad on a.id = ad.user_id where a.id > 10 and a.id < 50;" +
			"select a.id,a.username,a.password,a.age,a.sex,ad.score from user a right join user_score ad on a.id = ad.user_id where a.id > 10 and a.id < 50;" +
			"select a.id,a.username,a.password,a.age,a.sex,ad.score from user a left join user_score ad on a.id = ad.user_id where a.id in (select user_id from user_score where score > 90 and score < 99 ) " +
			"union select a.id,a.username,a.password,a.age,a.sex,ad.score from user a left join user_score ad on a.id = ad.user_id where a.id in (select user_id from user_score where score > 30 and score < 70 ); end",
		" CREATE PROCEDURE `proc_13`() begin select @a; begin select @b; end; end",
		" CREATE PROCEDURE `proc_14`() begin select @a; insert into t2 select * from t1; begin select @b;update t2 set id = 1; end; end",
		" CREATE PROCEDURE `proc_15`() begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881); select @a; insert into t2 select * from t1; begin declare s varchar(100); select @b;update t2 set id = 1; end; end",
		" CREATE PROCEDURE `proc_16`() begin declare s varchar(100) ; declare s int; declare s bigint;declare s float;declare s double;select @a; insert into t2 select * from t1; begin declare s varchar(100); select @b;update t2 set id = 1; end; end",
		" CREATE PROCEDURE `proc_17`() begin declare s char(100) ; declare s blob; declare s text;declare s DECIMAL(30,2);declare s datetime;select @a; insert into t2 select * from t1; begin declare s varchar(100); select @b;update t2 set id = 1; end; end",
		" CREATE PROCEDURE `proc_18`() begin declare s varchar(100) default s; declare s int default 1; declare s bigint default @a;select @a; insert into t2 select * from t1; begin declare s varchar(100); select @b;update t2 set id = 1; end; end",
		" CREATE PROCEDURE `proc_19`() begin select @a; insert into t2 select * from t1; begin declare s varchar(100);begin declare s varchar(100);begin declare s varchar(100); select @b;update t2 set id = 1; end; select @b;update t2 set id = 1; end; select @b;update t2 set id = 1; end; end",
	}
	sqlmod := " ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION "
	collateStr := " utf8mb4 utf8mb4_bin utf8mb4_bin"
	sql := "show create procedure "
	for i, testcase := range testcases {
		tk.MustExec(testcase)
		name := "proc_" + strconv.Itoa(i+1)
		tk.MustQuery(sql + name).Check(testkit.Rows(name + sqlmod + res[i] + collateStr))
		tk.MustExec("drop procedure " + name)
	}

	tk.MustGetErrCode("drop procedure proc_1", 1305)
	tk.MustExec("drop procedure if exists proc_1")
}

func TestBaseCall(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int)")
	tk.MustExec("create procedure t1() begin insert into t1 value(@a); end")
	tk.MustExec("set @a = 1")
	tk.MustExec("call t1")
	tk.MustQuery("select * from t1 order by id").Check(testkit.Rows("1"))
	tk.MustExec("set @a = 2")
	tk.MustExec("call t1()")
	tk.MustQuery("select * from t1 order by id").Check(testkit.Rows("1", "2"))
	tk.MustExec("create procedure t2() begin select * from t1 order by id; end")
	tk.MustExec("call t2")
	for _, res := range tk.Res {
		res.Check(testkit.Rows("1", "2"))
	}
	tk.ClearProcedureRes()
	tk.MustExec("call t2()")
	for _, res := range tk.Res {
		res.Check(testkit.Rows("1", "2"))
	}
	tk.ClearProcedureRes()
	tk.MustExec("create procedure t3() begin update t1 set id = id + 1; end")
	tk.MustExec("call t3")
	tk.MustExec("call t2")
	for _, res := range tk.Res {
		res.Check(testkit.Rows("2", "3"))
	}
	tk.ClearProcedureRes()
	tk.MustExec("call t2()")
	for _, res := range tk.Res {
		res.Check(testkit.Rows("2", "3"))
	}
	tk.ClearProcedureRes()
	tk.MustExec("call t3()")
	tk.MustExec("call t2")
	for _, res := range tk.Res {
		res.Check(testkit.Rows("3", "4"))
	}
	tk.ClearProcedureRes()
	tk.MustExec("call t2()")
	for _, res := range tk.Res {
		res.Check(testkit.Rows("3", "4"))
	}
	tk.ClearProcedureRes()
	tk.MustExec("create procedure t4() begin select * from t1 order by id; select * from t1 order by id; select * from t1 order by id; end")
	tk.MustExec("call t4()")
	require.Equal(t, 3, len(tk.Res))
	for _, res := range tk.Res {
		res.Check(testkit.Rows("3", "4"))
	}
	tk.ClearProcedureRes()
	tk.MustExec("create procedure t5() begin select * from t1 order by id; update t1 set id = id + 1; select * from t1 order by id; end")
	tk.MustExec("call t5()")
	require.Equal(t, 2, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("3", "4"))
	tk.Res[1].Check(testkit.Rows("4", "5"))
	tk.ClearProcedureRes()
	tk.MustExec("create procedure t6() begin select * from t1 order by id; update t1 set id = id + 1; begin update t1 set id = id + 1; select * from t1 order by id;insert into t1 value(1);end;select * from t1 order by id; end")
	tk.MustExec("call t6()")
	require.Equal(t, 3, len(tk.Res))
	tk.Res[0].Check(testkit.Rows("4", "5"))
	tk.Res[1].Check(testkit.Rows("6", "7"))
	tk.Res[2].Check(testkit.Rows("1", "6", "7"))
	tk.ClearProcedureRes()
	tk.MustExec("create table t2 (id int)")
	tk.MustExec("create procedure t7() insert into t2 select * from t1")
	tk.MustExec("call t7()")
	tk.MustQuery("select * from t2 order by id").Check(testkit.Rows("1", "6", "7"))
	tk.MustExec("truncate table t2")
	tk.MustExec("create procedure t8() insert into t2 select * from t1;insert into t2 select * from t1")
	tk.MustQuery("select * from t2 order by id").Check(testkit.Rows("1", "6", "7"))
	tk.MustQuery("show create procedure t8").Check(testkit.Rows("t8 ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION " +
		" CREATE PROCEDURE `t8`() insert into t2 select * from t1 utf8mb4 utf8mb4_bin utf8mb4_bin"))

}

func TestCallSelect(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE `user` ( `id` int(11) NOT NULL, `username` VARCHAR(30) DEFAULT NULL, `password` VARCHAR(30) DEFAULT NULL, " +
		"`age` int(11) NOT NULL, `sex` int(11) NOT NULL, PRIMARY KEY (`id`), KEY `username` (`username`) ) ENGINE=InnoDB;")
	tk.MustExec("CREATE TABLE `user_score` ( `id` int(11) NOT NULL, `subject` int(11) NOT NULL, `user_id` int(11) NOT NULL, " +
		"`score` int(11) NOT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB;")
	tk.MustExec("CREATE TABLE `user_address` ( `id` int(11) NOT NULL, `user_id` int(11) NOT NULL, `address` VARCHAR(30) DEFAULT NULL, " +
		"PRIMARY KEY (`id`), KEY `address` (`address`) ) ENGINE=InnoDB;")
	tk.InProcedure()
	tk.MustExec(`create procedure insert_data(i int, s_i int)  begin insert into user values(i, CONCAT("username-", i),CONCAT("password-", i),FLOOR( 15 + RAND() * 23),Mod(i,2));
    insert into user_score values(s_i, 1, i, FLOOR( 40 + i * 100));
    set s_i=s_i+1;
    insert into user_score values(s_i, 2, i, FLOOR( 40 + i * 100));
    set s_i=s_i+1;
    insert into user_score values(s_i, 3, i, FLOOR( 40 + i * 100));
    set s_i=s_i+1;
    insert into user_score values(s_i, 4, i, FLOOR( 40 + i * 100));
    set s_i=s_i+1;
    insert into user_score values(s_i, 5, i, FLOOR( 40 + i * 100));
    set s_i=s_i+1;
    insert into user_address values(i, i, CONCAT("useraddress-", i));
    set i=i+1;  end;`)
	for i := 1; i <= 100; i = i + 5 {
		sql := fmt.Sprintf("call insert_data(%d,%d)", i, i)
		tk.MustExec(sql)
	}

	tk.MustExec(`create procedure sp_select() begin
    select a.id,a.username,a.password,a.age,a.sex from user a where a.id > 10 and a.id < 50 order by a.id;

    select us.subject,count(us.user_id),sum(us.score),avg(us.score),max(us.score),min(us.score) from user_score us
        where us.score > 90 group by us.subject order by us.subject;

    select *,rank() over (partition by subject order by score desc) as ranking from user_score;

    select us.*,sum(us.score) over (order by us.id) as current_sum,
       avg(us.score) over (order by us.id) as current_avg,
       count(us.score) over (order by us.id) as current_count,
       max(us.score) over (order by us.id) as current_max,
       min(us.score) over (order by us.id) as current_min from user_score us;

    select us.*,sum(us.score) over (order by us.id) as current_sum,
       avg(us.score) over (order by us.id) as current_avg,
       count(us.score) over (order by us.id) as current_count,
       max(us.score) over (order by us.id) as current_max,
       min(us.score) over (order by us.id) as current_min,
       u.username ,ua.address,CONCAT(u.username, "-" ,ua.address) as userinfo
       from user_score us left join user u on u.id = us.user_id left join user_address ua on ua.id = us.user_id;

    SELECT DISTINCT us.user_id,u.username ,ua.address,CONCAT(u.username, "-" ,ua.address) as userinfo,
        sum(us.score) from user_score us left join user u on u.id = us.user_id
        left join user_address ua on ua.id = us.user_id group by us.user_id,u.username;

    select a.subject,a.id,a.user_id,u.username, a.score,a.rownum from ( select id,user_id,subject,score,row_number() over (order by score desc) as rownum from user_score) as a
        left join user u on a.user_id = u.id inner join user_score as b on a.id=b.id where a.rownum<=10 order by a.rownum ;

    select a.subject,a.id,a.score,a.rownum from (
        select id,subject,score,row_number() over (partition by subject order by score desc) as rownum from user_score) as a
        inner join user_score as b on a.id=b.id where a.rownum<=10 order by a.subject ;

    select *,u.username,ua.address,CONCAT(u.username, "-" ,ua.address) as userinfo,
        avg(us.score) over (order by us.id rows 2 preceding) as current_avg,
        sum(score) over (order by us.id rows 2 preceding) as current_sum from user_score us
        left join user u on u.id = us.user_id left join user_address ua on ua.id = us.user_id;

    select a.id,a.username,a.password,a.age,a.sex from user a where a.id in (select user_id from user_score where score > 90);

    select us.user_id,u.username,us.subject,us.score from user_score us left join user u on u.id = us.user_id
        where us.score > 90 group by us.user_id,us.subject,us.score;

    select us.user_id,u.username,us.subject,us.score from user_score us join user u on u.id = us.user_id
         where us.score > 90 group by us.user_id,us.subject,us.score;

    select a.id,a.username,a.password,a.age,a.sex,ad.address,CONCAT(a.username, "-" ,ad.address) as userinfo from user a
         left join user_address ad on a.id = ad.user_id where a.id > 10 and a.id < 50;

    select a.id,a.username,a.password,a.age,a.sex,ad.score from user a right join user_score ad on a.id = ad.user_id
        where a.id > 10 and a.id < 50;

    select a.id,a.username,a.password,a.age,a.sex,ad.score from user a
        left join user_score ad on a.id = ad.user_id where a.id in (select user_id from user_score where score > 90 and score < 99 )
        union select a.id,a.username,a.password,a.age,a.sex,ad.score from user a
        left join user_score ad on a.id = ad.user_id
        where a.id in (select user_id from user_score where score > 30 and score < 70 );
    end;
    `)
	tk.MustExec(`call sp_select`)
	require.Equal(t, 15, len(tk.Res))
	tk.MustQuery("select a.id,a.username,a.password,a.age,a.sex from user a where a.id > 10 and a.id < 50 order by a.id;").Check(tk.Res[0].Rows())
	tk.MustQuery(`select us.subject,count(us.user_id),sum(us.score),avg(us.score),max(us.score),min(us.score) from user_score us
	where us.score > 90 group by us.subject order by us.subject;`).Sort().Check(tk.Res[1].Sort().Rows())
	tk.MustQuery(`select *,rank() over (partition by subject order by score desc) as ranking from user_score;`).Sort().Check(tk.Res[2].Sort().Rows())
	tk.MustQuery(`select us.*,sum(us.score) over (order by us.id) as current_sum,
	avg(us.score) over (order by us.id) as current_avg,
	count(us.score) over (order by us.id) as current_count,
	max(us.score) over (order by us.id) as current_max,
	min(us.score) over (order by us.id) as current_min from user_score us;`).Sort().Check(tk.Res[3].Sort().Rows())
	tk.MustQuery(`select us.*,sum(us.score) over (order by us.id) as current_sum,
	avg(us.score) over (order by us.id) as current_avg,
	count(us.score) over (order by us.id) as current_count,
	max(us.score) over (order by us.id) as current_max,
	min(us.score) over (order by us.id) as current_min,
	u.username ,ua.address,CONCAT(u.username, "-" ,ua.address) as userinfo
	from user_score us left join user u on u.id = us.user_id left join user_address ua on ua.id = us.user_id;`).Sort().Check(tk.Res[4].Sort().Rows())
	tk.MustQuery(`SELECT DISTINCT us.user_id,u.username ,ua.address,CONCAT(u.username, "-" ,ua.address) as userinfo,
	sum(us.score) from user_score us left join user u on u.id = us.user_id
	left join user_address ua on ua.id = us.user_id group by us.user_id,u.username;`).Sort().Check(tk.Res[5].Sort().Rows())
	tk.MustQuery(`select a.subject,a.id,a.user_id,u.username, a.score,a.rownum from ( select id,user_id,subject,score,row_number() over (order by score desc) as rownum from user_score) as a
	left join user u on a.user_id = u.id inner join user_score as b on a.id=b.id where a.rownum<=10 order by a.rownum ;`).Sort().Check(tk.Res[6].Sort().Rows())
	tk.MustQuery(`select a.subject,a.id,a.score,a.rownum from (
        select id,subject,score,row_number() over (partition by subject order by score desc) as rownum from user_score) as a
        inner join user_score as b on a.id=b.id where a.rownum<=10 order by a.subject ;`).Sort().Check(tk.Res[7].Sort().Rows())
	tk.MustQuery(`select *,u.username,ua.address,CONCAT(u.username, "-" ,ua.address) as userinfo,
	avg(us.score) over (order by us.id rows 2 preceding) as current_avg,
	sum(score) over (order by us.id rows 2 preceding) as current_sum from user_score us
	left join user u on u.id = us.user_id left join user_address ua on ua.id = us.user_id;`).Sort().Check(tk.Res[8].Sort().Rows())
	tk.MustQuery(`select a.id,a.username,a.password,a.age,a.sex from user a where a.id in (select user_id from user_score where score > 90);`).Sort().Check(tk.Res[9].Sort().Rows())
	tk.MustQuery(`select us.user_id,u.username,us.subject,us.score from user_score us left join user u on u.id = us.user_id
	where us.score > 90 group by us.user_id,us.subject,us.score;`).Sort().Check(tk.Res[10].Sort().Rows())
	tk.MustQuery(`select us.user_id,u.username,us.subject,us.score from user_score us join user u on u.id = us.user_id
	where us.score > 90 group by us.user_id,us.subject,us.score;`).Sort().Check(tk.Res[11].Sort().Rows())
	tk.MustQuery(`select a.id,a.username,a.password,a.age,a.sex,ad.address,CONCAT(a.username, "-" ,ad.address) as userinfo from user a
	left join user_address ad on a.id = ad.user_id where a.id > 10 and a.id < 50;`).Sort().Check(tk.Res[12].Sort().Rows())
	tk.MustQuery(`select a.id,a.username,a.password,a.age,a.sex,ad.score from user a right join user_score ad on a.id = ad.user_id
	where a.id > 10 and a.id < 50;`).Sort().Check(tk.Res[13].Sort().Rows())
	tk.MustQuery(`select a.id,a.username,a.password,a.age,a.sex,ad.score from user a
	left join user_score ad on a.id = ad.user_id where a.id in (select user_id from user_score where score > 90 and score < 99 )
	union select a.id,a.username,a.password,a.age,a.sex,ad.score from user a
	left join user_score ad on a.id = ad.user_id
	where a.id in (select user_id from user_score where score > 30 and score < 70 );`).Sort().Check(tk.Res[14].Sort().Rows())
}

func TestSelect(t *testing.T) {

	testcases := []struct {
		name      string
		selectSQL string
	}{
		{
			"user_pro",
			"select a.id,a.username,a.password,a.age,a.sex " +
				"from user a where a.id > 10 and a.id < 50 order by id",
		},
		{
			"score_pro",
			"select us.subject,count(us.user_id),sum(us.score),avg(us.score),max(us.score),min(us.score) " +
				"from user_score us where us.score > 90 group by us.subject",
		},
		{
			"user_score_rank_pro",
			"select *,rank() over (partition by subject order by score desc) as ranking " +
				"from user_score",
		},
		{
			"user_win_pro",
			"select us.*,sum(us.score) over (order by us.id) as current_sum,avg(us.score) over (order by us.id) as current_avg," +
				"count(us.score) over (order by us.id) as current_count,max(us.score) over (order by us.id) as current_max," +
				"min(us.score) over (order by us.id) as current_min from user_score us",
		},
		{
			"user_win_join_pro",
			"select us.*,sum(us.score) over (order by us.id) as current_sum,avg(us.score) over (order by us.id) as current_avg," +
				"count(us.score) over (order by us.id) as current_count,max(us.score) over (order by us.id) as current_max," +
				"min(us.score) over (order by us.id) as current_min,u.username ,ua.address,CONCAT(u.username, \"-\" ,ua.address) as userinfo " +
				"from user_score us left join user u on u.id = us.user_id left join user_address ua on ua.id = us.user_id",
		},
		{
			"user_join_groupBy_pro",
			"SELECT DISTINCT us.user_id,u.username ,ua.address,CONCAT(u.username, \"-\" ,ua.address) as userinfo," +
				"sum(us.score) from user_score us left join user u on u.id = us.user_id left join user_address ua on ua.id = us.user_id " +
				"group by us.user_id,u.username order by us.user_id",
		},
		{
			"user_score_top10_pro",
			"select a.subject,a.id,a.user_id,u.username, a.score,a.rownum " +
				"from (" +
				"select id,user_id,subject,score,row_number() over (order by score desc) as rownum " +
				"from user_score) as a left join user u on a.user_id = u.id " +
				"inner join user_score as b on a.id=b.id " +
				"where a.rownum<=10 order by a.rownum",
		},
		{
			"user_fun_pro",
			"select *,u.username,ua.address,CONCAT(u.username, \"-\" ,ua.address) as userinfo," +
				"avg(us.score) over (order by us.id rows 2 preceding) as current_avg, " +
				"sum(score) over (order by us.id rows 2 preceding) as current_sum " +
				"from user_score us left join user u on u.id = us.user_id " +
				"left join user_address ua on ua.id = us.user_id order by u.id",
		},
		{
			"user_sub_sel_pro",
			"select a.id,a.username,a.password,a.age,a.sex " +
				"from user a " +
				"where a.id in (select user_id from user_score where score > 90) order by a.age desc,a.id",
		},
		{
			"user_left_join_groupBy_pro",
			"select users.subject,sum(users.score) " +
				"from (" +
				"select us.user_id,u.username,us.subject,us.score " +
				"from user_score us " +
				"left join user u on u.id = us.user_id where us.score > 90 ) as users " +
				"group by users.subject",
		},
		{
			"user_join_pro",
			"select users.subject,sum(users.score) " +
				"from (" +
				"select us.user_id,u.username,us.subject,us.score " +
				"from user_score us " +
				"join user u on u.id = us.user_id where us.score > 90 ) as users " +
				"group by users.subject",
		},
		{
			"user_left_join_pro",
			"select a.id,a.username,a.password,a.age,a.sex,ad.address," +
				"CONCAT(a.username, \"-\" ,ad.address) as userinfo " +
				"from user a " +
				"left join user_address ad on a.id = ad.user_id " +
				"where a.id > 10 and a.id < 50  order by a.id",
		},
		{
			"user_right_join_pro",
			"select a.id,a.username,a.password,a.age,a.sex,ad.score " +
				"from user a " +
				"right join user_score ad on a.id = ad.user_id " +
				"where a.id > 10 and a.id < 50 " +
				"order by ad.score desc,a.age",
		},
		{
			"union_pro",
			"select * " +
				"from (" +
				"select a.id,a.username,a.password,a.age,a.sex,ad.score " +
				"from user a " +
				"left join user_score ad on a.id = ad.user_id " +
				"where a.id in (" +
				"select user_id " +
				"from user_score " +
				"where score > 90 and score < 99 " +
				"order by ad.score desc,a.age) " +
				"union " +
				"select a.id,a.username,a.password,a.age,a.sex,ad.score " +
				"from user a " +
				"left join user_score ad on a.id = ad.user_id " +
				"where a.id in (" +
				"select user_id " +
				"from user_score " +
				"where score > 30 and score < 70)) user_info " +
				"order by user_info.score desc,user_info.age",
		},
		{
			"user_top10_pro",
			"select a.subject,a.id,a.score,a.rownum " +
				"from (" +
				"select id,subject,score,row_number() over (partition by subject order by score desc) as rownum " +
				"from user_score) as a inner join user_score as b on a.id=b.id where a.rownum<=10 order by a.subject",
		},
		{
			"user_info_pro",
			"select rank() over (partition by user_info.subject_1 order by user_info.score_1 desc) as ranking," +
				"avg(user_info.score_1) over (order by user_info.id rows 2 preceding) as current_avg," +
				"sum(user_info.score_1) over (order by user_info.id rows 2 preceding) as current_sum," +
				"sum(user_info.score_1) over (order by user_info.id) as score_1_sum," +
				"avg(user_info.score_1) over (order by user_info.id) as score_1_avg," +
				"count(user_info.score_1) over (order by user_info.id) as score_1_count," +
				"max(user_info.score_1) over (order by user_info.id) as score_1_max," +
				"min(user_info.score_1) over (order by user_info.id) as score_1_min," +
				"user_info.* " +
				"from (" +
				"select u.id,u.username,us1.subject as subject_1,us1.score as score_1,us2.subject as subject_2,us2.score as score_2," +
				"us3.subject as subject_3,us3.score as score_3,us4.subject as subject_4,us4.score as score_4,us5.subject as subject_5," +
				"us5.score as score_5,ua.address " +
				"from user u " +
				"left join user_score us1 on us1.user_id = u.id and us1.subject = 1 " +
				"left join user_score us2 on us2.user_id = u.id and us2.subject = 2 " +
				"left join user_score us3 on us3.user_id = u.id and us3.subject = 3 " +
				"left join user_score us4 on us4.user_id = u.id and us4.subject = 4 " +
				"left join user_score us5 on us5.user_id = u.id and us5.subject = 5 " +
				"left join test.user_address ua on u.id = ua.user_id) as user_info",
		},
	}
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	initEnv(tk)
	for _, tc := range testcases {
		pSql, cSQL := procedureSQL(tc.name, tc.selectSQL)
		runTestCases(t, store, pSql, cSQL, tc.selectSQL)
	}
	destroyEnv(tk)
}

func TestSelectInsert(t *testing.T) {
	testcases := []struct {
		name            string
		insertSelectSQL string
		selectSQL       string
	}{
		{
			"build_user_info_pro",
			"INSERT " +
				"INTO user_info (id,user_id,username,password,age,sex,address," +
				"subject_1,score_1," +
				"subject_2,score_2," +
				"subject_3,score_3," +
				"subject_4,score_4," +
				"subject_5,score_5) " +
				"SELECT * " +
				"FROM ( select u.id,us1.user_id,u.username,u.password,u.age,u.sex," +
				"ua.address, " +
				"us1.subject as subject_1,us1.score as score_1, " +
				"us2.subject as subject_2,us2.score as score_2, " +
				"us3.subject as subject_3,us3.score as score_3, " +
				"us4.subject as subject_4,us4.score as score_4, " +
				"us5.subject as subject_5,us5.score as score_5 " +
				"from " +
				"user u " +
				"left join user_score us1 on us1.user_id = u.id and us1.subject = 1 " +
				"left join user_score us2 on us2.user_id = u.id and us2.subject = 2 " +
				"left join user_score us3 on us3.user_id = u.id and us3.subject = 3 " +
				"left join user_score us4 on us4.user_id = u.id and us4.subject = 4 " +
				"left join user_score us5 on us5.user_id = u.id and us5.subject = 5 " +
				"left join test.user_address ua on u.id = ua.user_id) " +
				"as user_info",
			"select u.id,us1.user_id,u.username,u.password,u.age,u.sex,ua.address," +
				"us1.subject as subject_1,us1.score as score_1," +
				"us2.subject as subject_2,us2.score as score_2," +
				"us3.subject as subject_3,us3.score as score_3," +
				"us4.subject as subject_4,us4.score as score_4," +
				"us5.subject as subject_5,us5.score as score_5 " +
				"from user u " +
				"left join user_score us1 on us1.user_id = u.id and us1.subject = 1 " +
				"left join user_score us2 on us2.user_id = u.id and us2.subject = 2 " +
				"left join user_score us3 on us3.user_id = u.id and us3.subject = 3 " +
				"left join user_score us4 on us4.user_id = u.id and us4.subject = 4 " +
				"left join user_score us5 on us5.user_id = u.id and us5.subject = 5 " +
				"left join test.user_address ua on u.id = ua.user_id",
		},
	}
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	initEnv(tk)
	procedureSql, callSQL := procedureSQL(testcases[0].name, testcases[0].insertSelectSQL)
	newTk := testkit.NewTestKit(t, store)
	newTk.InProcedure()
	newTk.MustExec("use test")
	newTk.MustExec(procedureSql)
	newTk.MustExec(callSQL)
	userInfoRows := newTk.MustQuery("select * from user_info").Rows()
	selectRows := newTk.MustQuery(testcases[0].selectSQL).Rows()
	require.Equal(t, len(userInfoRows), len(selectRows))
	destroyEnv(tk)
}

func runTestCases(t *testing.T, store kv.Storage, procedure, runProcedure, selectSQL string) {
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")
	tk.MustExec(procedure)
	tk.MustExec(runProcedure)
	procedureRows := tk.Res[0].Rows()
	selectRows := tk.MustQuery(selectSQL).Rows()
	require.Equal(t, len(procedureRows), len(selectRows))
	require.Equal(t, procedureRows[0], selectRows[0], runProcedure)

}

func procedureSQL(procedureName, selectSQL string) (string, string) {
	sqlTemplate := "create procedure procedureName() begin selectSQL; end"
	sqlTemplate = strings.Replace(sqlTemplate, "procedureName", procedureName, 1)
	sqlTemplate = strings.Replace(sqlTemplate, "selectSQL", selectSQL, 1)

	callSqlTemplate := "call procedureName()"
	callSqlTemplate = strings.Replace(callSqlTemplate, "procedureName", procedureName, 1)
	return sqlTemplate, callSqlTemplate
}

func createTable(tk *testkit.TestKit) {
	tk.MustExec("CREATE TABLE IF NOT EXISTS `user` (`id` int(11) NOT NULL,`username` VARCHAR(30) DEFAULT NULL,`password` VARCHAR(30) DEFAULT NULL,`age` int(11) NOT NULL,`sex` int(11) NOT NULL,PRIMARY KEY (`id`),KEY `username` (`username`))")
	tk.MustExec("CREATE TABLE IF NOT EXISTS `user_score` (`id` int(11) NOT NULL,`subject` int(11) NOT NULL,`user_id` int(11) NOT NULL,`score` int(11) NOT NULL,PRIMARY KEY (`id`))")
	tk.MustExec("CREATE TABLE IF NOT EXISTS `user_address` (`id` int(11) NOT NULL,`user_id` int(11) NOT NULL,`address` VARCHAR(30) DEFAULT NULL,PRIMARY KEY (`id`),KEY `address` (`address`))")
	tk.MustExec("CREATE TABLE IF NOT EXISTS `user_info` (" +
		"`id` int(11) NOT NULL," +
		"`user_id` int(11) NOT NULL," +
		"`username` VARCHAR(30) DEFAULT NULL," +
		"`password` VARCHAR(30) DEFAULT NULL," +
		"`age` int(11) NOT NULL," +
		"`sex` int(11) NOT NULL," +
		"`address` VARCHAR(30) DEFAULT NULL," +
		"`subject_1` int(11) DEFAULT NULL,`score_1` int(11) DEFAULT NULL," +
		"`subject_2` int(11) DEFAULT NULL,`score_2` int(11) DEFAULT NULL," +
		"`subject_3` int(11) DEFAULT NULL,`score_3` int(11) DEFAULT NULL," +
		"`subject_4` int(11) DEFAULT NULL,`score_4` int(11) DEFAULT NULL," +
		"`subject_5` int(11) DEFAULT NULL,`score_5` int(11) DEFAULT NULL)")
}

func dropTable(tk *testkit.TestKit) {
	tk.MustExec("drop table IF EXISTS `user`")
	tk.MustExec("drop table IF EXISTS `user_score`")
	tk.MustExec("drop table IF EXISTS `user_address`")
	tk.MustExec("drop table IF EXISTS `user_info`")
}

func dropProcedure(tk *testkit.TestKit) {
	tk.MustExec("DROP PROCEDURE IF EXISTS user_pro")
	tk.MustExec("DROP PROCEDURE IF EXISTS score_pro")
	tk.MustExec("DROP PROCEDURE IF EXISTS user_score_rank_pro")
	tk.MustExec("DROP PROCEDURE IF EXISTS user_win_pro")
	tk.MustExec("DROP PROCEDURE IF EXISTS user_win_join_pro")
	tk.MustExec("DROP PROCEDURE IF EXISTS user_join_groupBy_pro")
	tk.MustExec("DROP PROCEDURE IF EXISTS user_score_top10_pro")
	tk.MustExec("DROP PROCEDURE IF EXISTS user_fun_pro")
	tk.MustExec("DROP PROCEDURE IF EXISTS user_sub_sel_pro")
	tk.MustExec("DROP PROCEDURE IF EXISTS user_left_join_groupBy_pro")
	tk.MustExec("DROP PROCEDURE IF EXISTS user_join_pro")
	tk.MustExec("DROP PROCEDURE IF EXISTS user_left_join_pro")
	tk.MustExec("DROP PROCEDURE IF EXISTS user_right_join_pro")
	tk.MustExec("DROP PROCEDURE IF EXISTS union_pro")
	tk.MustExec("DROP PROCEDURE IF EXISTS user_top10_pro")
	tk.MustExec("DROP PROCEDURE IF EXISTS user_info_pro")
	tk.MustExec("DROP PROCEDURE IF EXISTS build_user_info_pro")
}

func initEnv(tk *testkit.TestKit) {
	dropTable(tk)
	dropProcedure(tk)
	createTable(tk)
	tk.MustExec("CREATE PROCEDURE insert_user (IN id INTEGER) BEGIN insert into user values(id, CONCAT('username-', id),CONCAT('password-', id),FLOOR( 15 + RAND() * 23),Mod(id,2)); end")
	tk.MustExec("CREATE PROCEDURE insert_user_score(IN scoreId INTEGER,IN id INTEGER) BEGIN insert into user_score values(scoreId, 1, id, FLOOR( 40 + RAND() * 100)); end")
	tk.MustExec("CREATE PROCEDURE insert_user_address (IN id INTEGER) BEGIN insert into user_address values(id, id, CONCAT('useraddress-', id)); end")
	scoreId := 0
	for i := 0; i < 100; i++ {
		userSqlTemplate := "call insert_user(%?)"
		userSQL := new(strings.Builder)
		sqlexec.MustFormatSQL(userSQL, userSqlTemplate, i)
		tk.MustExec(userSQL.String())
		for j := 0; j < 10; j++ {
			userScoreSqlTemplate := "call insert_user_score(%?,%?)"
			userScoreSQL := new(strings.Builder)
			sqlexec.MustFormatSQL(userScoreSQL, userScoreSqlTemplate, scoreId, i)
			tk.MustExec(userScoreSQL.String())
			scoreId++
		}
		userAddressSqlTemplate := "call insert_user_address(%?)"
		userAddressSQL := new(strings.Builder)
		sqlexec.MustFormatSQL(userAddressSQL, userAddressSqlTemplate, i)
		tk.MustExec(userAddressSQL.String())
	}
}

func destroyEnv(tk *testkit.TestKit) {
	dropTable(tk)
	dropProcedure(tk)
}
