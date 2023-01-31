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
	"strconv"
	"testing"

	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestCreateProcedure(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	// no database
	tk.MustGetErrCode("create procedure sp_test() begin select@a; end;", 1046)
	tk.MustGetErrCode("create procedure test2.sp_test() begin select@a; end;", 1049)
	tk.MustExec("create procedure test.sp_test() begin select@a; end;")
	tk.MustQuery("show create procedure test.sp_test").Check(testkit.Rows("sp_test ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION " +
		" CREATE PROCEDURE `sp_test`() begin select@a; end; utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustExec("use test")
	tk.MustGetErrCode("create procedure sp_test() begin select@a; end;", 1304)
	tk.MustExec("create procedure if not exists sp_test() begin select@b; end;")
	tk.MustQuery("show create procedure test.sp_test").Check(testkit.Rows("sp_test ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION " +
		" CREATE PROCEDURE `sp_test`() begin select@a; end; utf8mb4 utf8mb4_bin utf8mb4_bin"))
	// in/out/inout
	tk.MustExec("create procedure if not exists sp_test1(id int) begin select@b; end;")
	tk.MustQuery("show create procedure sp_test1").Check(testkit.Rows("sp_test1 ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION " +
		" CREATE PROCEDURE `sp_test1`(id int) begin select@b; end; utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustExec("create procedure if not exists sp_test2(in id int) begin select@b; end;")
	tk.MustQuery("show create procedure sp_test2").Check(testkit.Rows("sp_test2 ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION " +
		" CREATE PROCEDURE `sp_test2`(in id int) begin select@b; end; utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustExec("create procedure if not exists sp_test3(out id int) begin select@b; end;")
	tk.MustQuery("show create procedure sp_test3").Check(testkit.Rows("sp_test3 ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION " +
		" CREATE PROCEDURE `sp_test3`(out id int) begin select@b; end; utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustExec("create procedure if not exists sp_test4(inout id int) begin select@b; end;")
	tk.MustQuery("show create procedure sp_test4").Check(testkit.Rows("sp_test4 ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION " +
		" CREATE PROCEDURE `sp_test4`(inout id int) begin select@b; end; utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustExec("create procedure if not exists sp_test5(id int,in id1 int,out id2 varchar(100),inout id3 int) begin select@b; end;")
	tk.MustQuery("show create procedure sp_test5").Check(testkit.Rows("sp_test5 ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION " +
		" CREATE PROCEDURE `sp_test5`(id int,in id1 int,out id2 varchar(100),inout id3 int) begin select@b; end; utf8mb4 utf8mb4_bin utf8mb4_bin"))
	tk.MustGetErrCode("create procedure sp_test6(in id int, out id int) begin select @a; end;", 1330)
	tk.MustGetErrCode("create procedure sp_test6() begin set a = 1; end;", 1193)
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
			"select a.id,a.username,a.password,a.age,a.sex from user a where a.id in (select user_id from user_score where score > 90);	end;",
		"create procedure proc_12() begin select us.user_id,u.username,us.subject,us.score from user_score us left join user u on u.id = us.user_id where us.score > 90 group by us.user_id,us.subject,us.score;" +
			"select us.user_id,u.username,us.subject,us.score from user_score us join user u on u.id = us.user_id where us.score > 90 group by us.user_id,us.subject,us.score;" +
			"select a.id,a.username,a.password,a.age,a.sex,ad.address,CONCAT(a.username, \"-\" ,ad.address) as userinfo from user a left join user_address ad on a.id = ad.user_id where a.id > 10 and a.id < 50;" +
			"select a.id,a.username,a.password,a.age,a.sex,ad.score from user a right join user_score ad on a.id = ad.user_id where a.id > 10 and a.id < 50;" +
			"select a.id,a.username,a.password,a.age,a.sex,ad.score from user a left join user_score ad on a.id = ad.user_id where a.id in (select user_id from user_score where score > 90 and score < 99 ) " +
			"union select a.id,a.username,a.password,a.age,a.sex,ad.score from user a left join user_score ad on a.id = ad.user_id where a.id in (select user_id from user_score where score > 30 and score < 70 ); end;",
	}
	res := []string{
		" CREATE PROCEDURE `proc_1`() begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END;",
		" CREATE PROCEDURE `proc_2`() begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END;",
		" CREATE PROCEDURE `proc_3`(in id int,inout id2 int,out id3 int) begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END;",
		" CREATE PROCEDURE `proc_4`(in id bigint,in id2 varchar(100),in id3 decimal(30,2)) begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END;",
		" CREATE PROCEDURE `proc_5`(in id double,in id2 float,out id3 char(10),in id4 binary) begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END;",
		" CREATE PROCEDURE `proc_6`(in id VARBINARY(30),in id2 BLOB,out id3 TEXT,in id4 ENUM('1','2')) begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END;",
		" CREATE PROCEDURE `proc_7`(in id SET('1','2')) begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select s;SELECT * FROM `t1`;SELECT * FROM `t2`;INSERT INTO `t1` VALUES (111);END;",
		" CREATE PROCEDURE `proc_8`(in id SET('1','2')) begin declare s varchar(100) DEFAULT FROM_UNIXTIME(1447430881);select a.id,a.username,a.password,a.age,a.sex from user a where a.id > 10 and a.id < 50;" +
			"select us.subject,count(us.user_id),sum(us.score),avg(us.score),max(us.score),min(us.score) from user_score us where us.score > 90 group by us.subject;END;",
		" CREATE PROCEDURE `proc_9`(in id SET('1','2')) begin select *,rank() over (partition by subject order by score desc) as ranking from user_score;select *,rank() over (partition by subject order by score desc) as ranking from user_score;end",
		" CREATE PROCEDURE `proc_10`(in id SET('1','2')) begin select us.*,sum(us.score) over (order by us.id) as current_sum," +
			"avg(us.score) over (order by us.id) as current_avg,count(us.score) over (order by us.id) as current_count,max(us.score) over (order by us.id) as current_max,min(us.score) over (order by us.id) as current_min from user_score us;" +
			"select us.*,sum(us.score) over (order by us.id) as current_sum, avg(us.score) over (order by us.id) as current_avg,count(us.score) over (order by us.id) as current_count,max(us.score) over (order by us.id) as current_max," +
			"min(us.score) over (order by us.id) as current_min,u.username ,ua.address,CONCAT(u.username, \"-\" ,ua.address) as userinfo from user_score us left join user u on u.id = us.user_id left join user_address ua on ua.id = us.user_id; end;",
		" CREATE PROCEDURE `proc_11`() begin SELECT DISTINCT us.user_id,u.username ,ua.address,CONCAT(u.username, \"-\" ,ua.address) as userinfo, sum(us.score) from user_score us left join user u on u.id = us.user_id" +
			"left join user_address ua on ua.id = us.user_id group by us.user_id,u.username;" +
			"select a.subject,a.id,a.user_id,u.username, a.score,a.rownum from (select id,user_id,subject,score,row_number() over (order by score desc) as rownum from user_score) as a left join user u on a.user_id = u.id" +
			"inner join user_score as b on a.id=b.id where a.rownum<=10 order by a.rownum ;" +
			"select a.subject,a.id,a.score,a.rownum from (" +
			"select id,subject,score,row_number() over (partition by subject order by score desc) as rownum from user_score) as a inner join user_score as b on a.id=b.id where a.rownum<=10 order by a.subject ;" +
			"select *,u.username,ua.address,CONCAT(u.username, \"-\" ,ua.address) as userinfo,avg(us.score) over (order by us.id rows 2 preceding) as current_avg,sum(score) over (order by us.id rows 2 preceding) as current_sum from user_score us" +
			" left join user u on u.id = us.user_id left join user_address ua on ua.id = us.user_id;" +
			"select a.id,a.username,a.password,a.age,a.sex from user a where a.id in (select user_id from user_score where score > 90);	end;",
		" CREATE PROCEDURE `proc_12`() begin select us.user_id,u.username,us.subject,us.score from user_score us left join user u on u.id = us.user_id where us.score > 90 group by us.user_id,us.subject,us.score;" +
			"select us.user_id,u.username,us.subject,us.score from user_score us join user u on u.id = us.user_id where us.score > 90 group by us.user_id,us.subject,us.score;" +
			"select a.id,a.username,a.password,a.age,a.sex,ad.address,CONCAT(a.username, \"-\" ,ad.address) as userinfo from user a left join user_address ad on a.id = ad.user_id where a.id > 10 and a.id < 50;" +
			"select a.id,a.username,a.password,a.age,a.sex,ad.score from user a right join user_score ad on a.id = ad.user_id where a.id > 10 and a.id < 50;" +
			"select a.id,a.username,a.password,a.age,a.sex,ad.score from user a left join user_score ad on a.id = ad.user_id where a.id in (select user_id from user_score where score > 90 and score < 99 ) " +
			"union select a.id,a.username,a.password,a.age,a.sex,ad.score from user a left join user_score ad on a.id = ad.user_id where a.id in (select user_id from user_score where score > 30 and score < 70 ); end;",
	}
	sqlmod := " ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION "
	collateStr := " utf8mb4 utf8mb4_bin utf8mb4_bin"
	sql := "show create procedure "
	for i, testcase := range testcases {
		tk.MustExec(testcase)
		name := "proc_" + strconv.Itoa(i+1)
		tk.MustQuery(sql + name).Check(testkit.Rows(name + sqlmod + res[i] + collateStr))
	}

	tk.MustExec("drop procedure proc_1")
	tk.MustGetErrCode("drop procedure proc_1", 1305)
	tk.MustExec("drop procedure if exists proc_1")
}
