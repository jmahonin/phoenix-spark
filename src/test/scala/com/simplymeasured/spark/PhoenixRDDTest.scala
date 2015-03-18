/*
   Copyright 2014 Simply Measured, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package com.simplymeasured.spark

import java.sql.DriverManager

import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.phoenix.schema.types.PVarchar
import org.apache.phoenix.schema.{ColumnNotFoundException}
import org.apache.phoenix.util.ColumnInfo
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StringType, StructField}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class PhoenixRDDTest extends FunSuite with Matchers with BeforeAndAfterAll {
  lazy val hbaseTestingUtility = {
    new HBaseTestingUtility()
  }

  lazy val hbaseConfiguration = {
    val conf = hbaseTestingUtility.getConfiguration

    val quorum = conf.get("hbase.zookeeper.quorum")
    val clientPort = conf.get("hbase.zookeeper.property.clientPort")
    val znodeParent = conf.get("zookeeper.znode.parent")

    // This is an odd one - the Zookeeper Quorum entry in the config is totally wrong. It's
    // just reporting localhost.
    conf.set(org.apache.hadoop.hbase.HConstants.ZOOKEEPER_QUORUM, s"$quorum:$clientPort:$znodeParent")

    conf
  }

  lazy val quorumAddress = {
    hbaseConfiguration.get("hbase.zookeeper.quorum")
  }

  lazy val zookeeperClientPort = {
    hbaseConfiguration.get("hbase.zookeeper.property.clientPort")
  }

  lazy val zookeeperZnodeParent = {
    hbaseConfiguration.get("zookeeper.znode.parent")
  }

  lazy val hbaseConnectionString = {
    s"$quorumAddress:$zookeeperClientPort:$zookeeperZnodeParent"
  }

  override def beforeAll() {
    hbaseTestingUtility.startMiniCluster()

    val conn = DriverManager.getConnection(s"jdbc:phoenix:$hbaseConnectionString")

    conn.setAutoCommit(true)

    // each SQL statement used to set up Phoenix must be on a single line. Yes, that
    // can potentially make large lines.
    val setupSqlSource = getClass.getClassLoader.getResourceAsStream("setup.sql")

    val setupSql = scala.io.Source.fromInputStream(setupSqlSource).getLines()

    for (sql <- setupSql) {
      val stmt = conn.createStatement()

      stmt.execute(sql)

      stmt.close()
    }

    conn.commit()
  }

  override def afterAll() {
    hbaseTestingUtility.shutdownMiniCluster()
  }

  val conf = new SparkConf()

  val sc = new SparkContext("local[1]", "PhoenixSparkTest", conf)

  test("Can create valid SQL") {
    val rdd = PhoenixRDD.NewPhoenixRDD(sc, "MyTable", Array("Foo", "Bar"),
      conf = hbaseConfiguration)

    rdd.buildSql("MyTable", Array("Foo", "Bar"), None) should
      equal("SELECT \"Foo\", \"Bar\" FROM \"MyTable\"")
  }

  test("Can convert Phoenix schema") {
    val phoenixSchema = List(
      new ColumnInfo("varcharColumn", PVarchar.INSTANCE.getSqlType)
    )

    val rdd = PhoenixRDD.NewPhoenixRDD(sc, "MyTable", Array("Foo", "Bar"),
      conf = hbaseConfiguration)

    val catalystSchema = rdd.phoenixSchemaToCatalystSchema(phoenixSchema)

    val expected = List(StructField("varcharColumn", StringType, nullable = true))

    catalystSchema shouldEqual expected
  }

  test("Can create schema RDD and execute query") {
    val sqlContext = new SQLContext(sc)

    val rdd1 = PhoenixRDD.NewPhoenixRDD(sc, "TABLE1", Array("ID", "COL1"), conf = hbaseConfiguration)

    val dataFrame1 = rdd1.toDataFrame(sqlContext)

    dataFrame1.registerTempTable("sql_table_1")

    val rdd2 = PhoenixRDD.NewPhoenixRDD(sc, "TABLE2", Array("ID", "TABLE1_ID"),
      conf = hbaseConfiguration)

    val schemaRDD2 = rdd2.toDataFrame(sqlContext)

    schemaRDD2.registerTempTable("sql_table_2")

    val sqlRdd = sqlContext.sql("SELECT t1.ID, t1.COL1, t2.ID, t2.TABLE1_ID FROM sql_table_1 AS t1 INNER JOIN sql_table_2 AS t2 ON (t2.TABLE1_ID = t1.ID)")

    val count = sqlRdd.count()

    count shouldEqual 6L
  }

  test("Can create schema RDD and execute query on case sensitive table") {
    val sqlContext = new SQLContext(sc)

    val rdd1 = PhoenixRDD.NewPhoenixRDD(sc, "table3", Array("id", "col1"), conf = hbaseConfiguration)

    val schemaRDD1 = rdd1.toDataFrame(sqlContext)

    schemaRDD1.registerTempTable("table3")

    val sqlRdd = sqlContext.sql("SELECT * FROM table3")

    val count = sqlRdd.count()

    count shouldEqual 2L
  }

  test("Can create schema RDD and execute constrained query") {
    val sqlContext = new SQLContext(sc)

    val rdd1 = PhoenixRDD.NewPhoenixRDD(sc, "TABLE1", Array("ID", "COL1"), conf = hbaseConfiguration)

    val schemaRDD1 = rdd1.toDataFrame(sqlContext)

    schemaRDD1.registerTempTable("sql_table_1")

    val rdd2 = PhoenixRDD.NewPhoenixRDD(sc, "TABLE2", Array("ID", "TABLE1_ID"),
      predicate = Some("\"ID\" = 1"),
      conf = hbaseConfiguration)

    val schemaRDD2 = rdd2.toDataFrame(sqlContext)

    schemaRDD2.registerTempTable("sql_table_2")

    val sqlRdd = sqlContext.sql("SELECT t1.ID, t1.COL1, t2.ID, t2.TABLE1_ID FROM sql_table_1 AS t1 INNER JOIN sql_table_2 AS t2 ON (t2.TABLE1_ID = t1.ID)")

    val count = sqlRdd.count()

    count shouldEqual 1L
  }

  test("Using a predicate referring to a non-existent column should fail") {
    intercept[RuntimeException] {
      val sqlContext = new SQLContext(sc)

      val rdd1 = PhoenixRDD.NewPhoenixRDD(sc, "table3", Array("id", "col1"),
        predicate = Some("foo = bar"),
        conf = hbaseConfiguration)

      val schemaRDD1 = rdd1.toDataFrame(sqlContext)

      schemaRDD1.registerTempTable("table3")

      val sqlRdd = sqlContext.sql("SELECT * FROM table3")

      // we have to execute an action before the predicate failure can occur
      val count = sqlRdd.count()
    }.getCause shouldBe a [ColumnNotFoundException]
  }

  test("Can create schema RDD with predicate that will never match") {
    val sqlContext = new SQLContext(sc)

    val rdd1 = PhoenixRDD.NewPhoenixRDD(sc, "table3", Array("id", "col1"),
      predicate = Some("\"id\" = -1"),
      conf = hbaseConfiguration)

    val schemaRDD1 = rdd1.toDataFrame(sqlContext)

    schemaRDD1.registerTempTable("table3")

    val sqlRdd = sqlContext.sql("SELECT * FROM table3")

    val count = sqlRdd.count()

    count shouldEqual 0L
  }

  test("Can create schema RDD with complex predicate") {
    val sqlContext = new SQLContext(sc)

    val rdd1 = PhoenixRDD.NewPhoenixRDD(sc, "DATE_PREDICATE_TEST_TABLE", Array("ID", "TIMESERIES_KEY"),
      predicate = Some("ID > 0 AND TIMESERIES_KEY BETWEEN CAST(TO_DATE('1990-01-01 00:00:01', 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) AND CAST(TO_DATE('1990-01-30 00:00:01', 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP)"),
      conf = hbaseConfiguration)

    val schemaRDD1 = rdd1.toDataFrame(sqlContext)

    schemaRDD1.registerTempTable("date_predicate_test_table")

    val sqlRdd = sqlContext.sql("SELECT * FROM date_predicate_test_table")

    val count = sqlRdd.count()

    count shouldEqual 0L
  }

  test("Can query an array table") {
    val sqlContext = new SQLContext(sc)

    val rdd1 = PhoenixRDD.NewPhoenixRDD(sc, "ARRAY_TEST_TABLE", Array("ID", "VCARRAY"),
      conf = hbaseConfiguration)

    val schemaRDD1 = rdd1.toDataFrame(sqlContext)

    schemaRDD1.registerTempTable("ARRAY_TEST_TABLE")

    val sqlRdd = sqlContext.sql("SELECT * FROM ARRAY_TEST_TABLE")

    val count = sqlRdd.count()

    // get row 0, column 1, which should be "VCARRAY"
    val arrayValues = sqlRdd.collect().apply(0).apply(1)

    arrayValues should equal(Array("String1", "String2", "String3"))

    count shouldEqual 1L
  }
}
