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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.phoenix.pig.PhoenixPigConfiguration
import org.apache.phoenix.pig.PhoenixPigConfiguration.SchemaType
import org.apache.phoenix.pig.hadoop.{PhoenixInputFormat, PhoenixRecord}
import org.apache.phoenix.schema.PDataType
import org.apache.phoenix.util.ColumnInfo
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.{SQLContext, SchemaRDD}

import scala.collection.JavaConverters._

class PhoenixRDD(sc: SparkContext, host: String, table: String,
                 columns: Seq[String], predicate: Option[String] = None,
                 batchSize: Long = 100, @transient conf: Configuration)
  extends RDD[PhoenixRecord](sc, Nil) with Logging {

  val hadoopConf = new SerializableWritable(conf)

  @transient lazy val phoenixConf = {
    getPhoenixConfiguration
  }

  val phoenixRDD = sc.newAPIHadoopRDD(phoenixConf.getConfiguration,
    classOf[PhoenixInputFormat],
    classOf[NullWritable],
    classOf[PhoenixRecord])

  override protected def getPartitions: Array[Partition] = {
    phoenixRDD.partitions
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext) = {
    phoenixRDD.compute(split, context).map(r => r._2)
  }

  def printPhoenixConfig {
    val conf = phoenixConf

    for (mapEntry <- conf.getConfiguration.iterator().asScala) {
      val k = mapEntry.getKey
      val v = mapEntry.getValue

      if (k.startsWith("phoenix")) {
        println(s"$k = $v")
      }
    }
  }

  def getPhoenixConfiguration = {
    // This is just simply not serializable, so don't try.
    val phoenixConf = new PhoenixPigConfiguration(new Configuration(hadoopConf.value))

    phoenixConf.setSelectStatement(buildSql(table, columns, predicate))
    phoenixConf.setSchemaType(SchemaType.QUERY)
    phoenixConf.setSelectColumns(columns.mkString(","))

    phoenixConf.configure(host, "\"" + table + "\"", batchSize)

    phoenixConf
  }

  def buildSql(table: String, columns: Seq[String], predicate: Option[String]): String = {
    val query = ("SELECT %s FROM \"%s\"" format(columns.map(f => "\"" + f + "\"").mkString(", "), table))

    query + (predicate match {
      case Some(pred: String) => " " + pred
      case _ => ""
    })
  }

  def toSchemaRDD(sqlContext: SQLContext): SchemaRDD = {
    val columnList = phoenixConf.getSelectColumnMetadataList

    val structFields = phoenixSchemaToCatalystSchema(columnList.asScala)

    sqlContext.applySchema(map(pr => {
      val values = pr.getValues.asScala

      val r = new GenericMutableRow(values.length)

      var i = 0
      while (i < values.length) {
        r.update(i, values(i))

        i += 1
      }

      r
    }), new StructType(structFields))
  }

  def phoenixSchemaToCatalystSchema(columnList: Seq[ColumnInfo]) = {
    columnList.map(ci => {
      val structType = phoenixTypeToCatalystType(ci.getPDataType)

      StructField(ci.getDisplayName, structType, nullable = true)
    })
  }

  def phoenixTypeToCatalystType(phoenixType: PDataType) = {
    phoenixType match {
      case PDataType.VARCHAR | PDataType.CHAR =>
        StringType
      case PDataType.LONG | PDataType.UNSIGNED_LONG =>
        LongType
      case PDataType.INTEGER | PDataType.UNSIGNED_INT =>
        IntegerType
      case PDataType.SMALLINT | PDataType.UNSIGNED_SMALLINT =>
        ShortType
      case PDataType.TINYINT | PDataType.UNSIGNED_TINYINT =>
        ByteType
      case PDataType.FLOAT | PDataType.UNSIGNED_FLOAT =>
        FloatType
      case PDataType.DOUBLE | PDataType.UNSIGNED_DOUBLE =>
        DoubleType
      case PDataType.DECIMAL =>
        DecimalType
      case PDataType.TIMESTAMP | PDataType.UNSIGNED_TIMESTAMP =>
        TimestampType
      case PDataType.TIME | PDataType.UNSIGNED_TIME =>
        TimestampType
      case PDataType.DATE | PDataType.UNSIGNED_DATE =>
        TimestampType
      case PDataType.BOOLEAN =>
        BooleanType
      case PDataType.VARBINARY | PDataType.BINARY =>
        BinaryType
      case PDataType.INTEGER_ARRAY | PDataType.UNSIGNED_INT_ARRAY =>
        ArrayType(IntegerType, containsNull = true)
      case PDataType.BOOLEAN_ARRAY =>
        ArrayType(BooleanType, containsNull = true)
      case PDataType.VARCHAR_ARRAY | PDataType.CHAR_ARRAY =>
        ArrayType(StringType, containsNull = true)
      case PDataType.VARBINARY_ARRAY | PDataType.BINARY_ARRAY =>
        ArrayType(BinaryType, containsNull = true)
      case PDataType.LONG_ARRAY | PDataType.UNSIGNED_LONG_ARRAY =>
        ArrayType(LongType, containsNull = true)
      case PDataType.SMALLINT_ARRAY | PDataType.UNSIGNED_SMALLINT_ARRAY =>
        ArrayType(IntegerType, containsNull = true)
      case PDataType.TINYINT_ARRAY | PDataType.UNSIGNED_TINYINT_ARRAY =>
        ArrayType(ByteType, containsNull = true)
      case PDataType.FLOAT_ARRAY | PDataType.UNSIGNED_FLOAT_ARRAY =>
        ArrayType(FloatType, containsNull = true)
      case PDataType.DOUBLE_ARRAY | PDataType.UNSIGNED_DOUBLE_ARRAY =>
        ArrayType(DoubleType, containsNull = true)
      case PDataType.DECIMAL_ARRAY =>
        ArrayType(DecimalType, containsNull = true)
      case PDataType.TIMESTAMP_ARRAY | PDataType.UNSIGNED_TIMESTAMP_ARRAY =>
        ArrayType(TimestampType, containsNull = true)
      case PDataType.DATE_ARRAY | PDataType.UNSIGNED_DATE_ARRAY =>
        ArrayType(TimestampType, containsNull = true)
      case PDataType.TIME_ARRAY | PDataType.UNSIGNED_TIME_ARRAY =>
        ArrayType(TimestampType, containsNull = true)
    }
  }
}

object PhoenixRDD {
  def NewPhoenixRDD(sc: SparkContext, host: String, table: String,
                    columns: Seq[String], batchSize: Long = 100,
                    conf: Configuration) = {
    new PhoenixRDD(sc, host, table, columns, None, batchSize, conf)
  }
  def NewPhoenixRDDWithPredicate(sc: SparkContext, host: String, table: String,
                    columns: Seq[String], predicate: String,
                    batchSize: Long = 100, conf: Configuration) = {
    new PhoenixRDD(sc, host, table, columns, Some(predicate), batchSize, conf)
  }
}