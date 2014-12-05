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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.hadoop.io.NullWritable
import org.apache.phoenix.pig.PhoenixPigConfiguration
import org.apache.phoenix.pig.PhoenixPigConfiguration.SchemaType
import org.apache.phoenix.pig.hadoop.{PhoenixInputFormat, PhoenixRecord}
import org.apache.phoenix.schema.PDataType
import org.apache.phoenix.util.ColumnInfo
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.types.{StringType, StructField}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.collection.JavaConverters._

class PhoenixRDDTest extends FunSuite with Matchers with BeforeAndAfterAll {
  lazy val hbaseTestingUtility = {
    new HBaseTestingUtility()
  }

  lazy val hbaseConfiguration = {
    hbaseTestingUtility.getConfiguration
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
    val rdd = PhoenixRDD.NewPhoenixRDD(sc, "localhost", "MyTable", Array("Foo", "Bar"),
      conf = new Configuration())

    rdd.buildSql("MyTable", Array("Foo", "Bar"), None) should equal("SELECT \"Foo\", \"Bar\" FROM \"MyTable\"")
  }

  test("Can create valid SQL with predicate") {
    val rdd = PhoenixRDD.NewPhoenixRDDWithPredicate(sc, "localhost", "MyTable", Array("Foo", "Bar"),
      "WHERE Foo = Bar", conf = new Configuration())

    rdd.buildSql("MyTable", Array("Foo", "Bar"), Some("WHERE Foo = Bar")) should equal("SELECT \"Foo\", \"Bar\" FROM \"MyTable\" WHERE Foo = Bar")
  }

  test("Can convert Phoenix schema") {
    val phoenixSchema = List(
      new ColumnInfo("varcharColumn", PDataType.VARCHAR.getSqlType)
    )

    val rdd = PhoenixRDD.NewPhoenixRDD(sc, "localhost", "MyTable", Array("Foo", "Bar"),
      conf = new Configuration())

    val catalystSchema = rdd.phoenixSchemaToCatalystSchema(phoenixSchema)

    val expected = List(StructField("varcharColumn", StringType, nullable = true))

    catalystSchema shouldEqual expected
  }

  test("Can create schema RDD and execute query") {
    val sqlContext = new SQLContext(sc)

    val rdd1 = PhoenixRDD.NewPhoenixRDD(sc, hbaseConnectionString,
      "TABLE1", Array("ID", "COL1"), conf = hbaseConfiguration)

    val schemaRDD1 = rdd1.toSchemaRDD(sqlContext)

    schemaRDD1.registerTempTable("sql_table_1")

    val rdd2 = PhoenixRDD.NewPhoenixRDD(sc, hbaseConnectionString,
      "TABLE2", Array("ID", "TABLE1_ID"),
      conf = hbaseConfiguration)

    val schemaRDD2 = rdd2.toSchemaRDD(sqlContext)

    schemaRDD2.registerTempTable("sql_table_2")

    val sqlRdd = sqlContext.sql("SELECT t1.ID, t1.COL1, t2.ID, t2.TABLE1_ID FROM sql_table_1 AS t1 INNER JOIN sql_table_2 AS t2 ON (t2.TABLE1_ID = t1.ID)")

    val count = sqlRdd.count()

    count shouldEqual 6L
  }

  test("Can create schema RDD and execute query on case sensitive table") {
    val sqlContext = new SQLContext(sc)

    val rdd1 = PhoenixRDD.NewPhoenixRDD(sc, hbaseConnectionString,
      "table3", Array("id", "col1"), conf = hbaseConfiguration)

    val schemaRDD1 = rdd1.toSchemaRDD(sqlContext)

    schemaRDD1.registerTempTable("table3")

    val sqlRdd = sqlContext.sql("SELECT * FROM table3")

    val count = sqlRdd.count()

    count shouldEqual 2L
  }

  // Waiting on PHOENIX-1461
  ignore("Direct query of an array table") {
    val phoenixConf = new PhoenixPigConfiguration(hbaseConfiguration)

    phoenixConf.setSelectStatement("SELECT * FROM ARRAY_TEST_TABLE")
    phoenixConf.setSelectColumns("ID,VCARRAY")
    phoenixConf.setSchemaType(SchemaType.QUERY)
    phoenixConf.configure(hbaseConnectionString, "ARRAY_TEST_TABLE", 100)

    val columns = phoenixConf.getSelectColumnMetadataList

    for (column <- columns.asScala) {
      println(column.getPDataType)
    }

    val phoenixRDD = sc.newAPIHadoopRDD(phoenixConf.getConfiguration,
      classOf[PhoenixInputFormat],
      classOf[NullWritable],
      classOf[PhoenixRecord])

    val count = phoenixRDD.count()

    count shouldEqual 1L
  }

  ignore("Can query an array table") {
    val sqlContext = new SQLContext(sc)

    val rdd1 = PhoenixRDD.NewPhoenixRDD(sc, hbaseConnectionString,
      "ARRAY_TEST_TABLE", Array("ID", "VCARRAY"), conf = hbaseConfiguration)

    val schemaRDD1 = rdd1.toSchemaRDD(sqlContext)

    schemaRDD1.registerTempTable("ARRAY_TEST_TABLE")

    val sqlRdd = sqlContext.sql("SELECT * FROM ARRAY_TEST_TABLE")

    val count = sqlRdd.count()

    count shouldEqual 1L
  }
}
