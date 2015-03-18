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
import org.apache.phoenix.mapreduce.PhoenixInputFormat
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil
import org.apache.phoenix.schema.types._
import org.apache.phoenix.util.ColumnInfo
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.apache.spark.sql.types._
import scala.collection.JavaConverters._

class PhoenixRDD(sc: SparkContext, table: String, columns: Seq[String],
                 predicate: Option[String] = None, @transient conf: Configuration)
  extends RDD[PhoenixRecordWritable](sc, Nil) with Logging {

  @transient lazy val phoenixConf = {
    getPhoenixConfiguration
  }

  val phoenixRDD = sc.newAPIHadoopRDD(phoenixConf,
    classOf[PhoenixInputFormat[PhoenixRecordWritable]],
    classOf[NullWritable],
    classOf[PhoenixRecordWritable])

  override protected def getPartitions: Array[Partition] = {
    phoenixRDD.partitions
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext) = {
    phoenixRDD.compute(split, context).map(r => r._2)
  }

  def printPhoenixConfig(conf: Configuration): Unit = {
    for (mapEntry <- conf.iterator().asScala) {
      val k = mapEntry.getKey
      val v = mapEntry.getValue

      if (k.startsWith("phoenix")) {
        println(s"$k = $v")
      }
    }
  }

  def getPhoenixConfiguration: Configuration = {
    // This is just simply not serializable, so don't try, but clone it because
    // PhoenixConfigurationUtil mutates it.
    val config = new Configuration(conf)

    PhoenixConfigurationUtil.setInputQuery(config, buildSql(table, columns, predicate))
    PhoenixConfigurationUtil.setSelectColumnNames(config, columns.mkString(","))
    PhoenixConfigurationUtil.setInputTableName(config, "\"" + table + "\"")
    PhoenixConfigurationUtil.setInputClass(config, classOf[PhoenixRecordWritable])

    config
  }

  def buildSql(table: String, columns: Seq[String], predicate: Option[String]): String = {
    val query = "SELECT %s FROM \"%s\"" format(columns.map(f => "\"" + f + "\"").mkString(", "), table)

    query + (predicate match {
      case Some(p: String) => " WHERE " + p
      case _ => ""
    })
  }

  def toDataFrame(sqlContext: SQLContext): DataFrame = {
    val columnList = PhoenixConfigurationUtil.getSelectColumnMetadataList(new Configuration(phoenixConf)).asScala

    // The Phoenix ColumnInfo class is not serializable, but a Seq[String] is.
    val columnNames: Seq[String] = columnList.map(ci => {
      ci.getDisplayName
    })

    val structFields = phoenixSchemaToCatalystSchema(columnList).toArray

    sqlContext.createDataFrame(map(pr => {
      val values = pr.resultMap

      val r = new GenericMutableRow(values.size)

      var i = 0
      for (columnName <- columnNames) {
        r.update(i, values(columnName))

        i += 1
      }

      r.asInstanceOf[Row]
    }), new StructType(structFields))
  }

  def phoenixSchemaToCatalystSchema(columnList: Seq[ColumnInfo]) = {
    columnList.map(ci => {
      val structType = phoenixTypeToCatalystType(ci.getPDataType)

      StructField(ci.getDisplayName, structType)
    })
  }

  // 4.3.0 broke up the PDataType enum, and due to type erasure, it's difficult
  // to match on them at runtime. There must be a better way, but all I could
  // come up with is this lookup table to load the class names so we can use
  // match() on a stable identifier.

  object PDataTypeTable extends Serializable {
    val VARCHAR = PVarchar.INSTANCE.getJavaClassName
    val CHAR = PChar.INSTANCE.getJavaClassName
    val LONG = PLong.INSTANCE.getJavaClassName
    val UNSIGNED_LONG = PUnsignedLong.INSTANCE.getJavaClassName
    val INTEGER = PInteger.INSTANCE.getJavaClassName
    val UNSIGNED_INT = PUnsignedInt.INSTANCE.getJavaClassName
    val SMALLINT = PSmallint.INSTANCE.getJavaClassName
    val UNSIGNED_SMALLINT = PUnsignedSmallint.INSTANCE.getJavaClassName
    val TINYINT = PTinyint.INSTANCE.getJavaClassName
    val UNSIGNED_TINYINT = PUnsignedTinyint.INSTANCE.getJavaClassName
    val FLOAT = PFloat.INSTANCE.getJavaClassName
    val UNSIGNED_FLOAT = PUnsignedFloat.INSTANCE.getJavaClassName
    val DOUBLE = PDouble.INSTANCE.getJavaClassName
    val UNSIGNED_DOUBLE = PUnsignedDouble.INSTANCE.getJavaClassName
    val DECIMAL = PDecimal.INSTANCE.getJavaClassName
    val TIMESTAMP = PTimestamp.INSTANCE.getJavaClassName
    val UNSIGNED_TIMESTAMP = PUnsignedTimestamp.INSTANCE.getJavaClassName
    val TIME = PTime.INSTANCE.getJavaClassName
    val UNSIGNED_TIME = PUnsignedTime.INSTANCE.getJavaClassName
    val DATE = PDate.INSTANCE.getJavaClassName
    val UNSIGNED_DATE = PUnsignedDate.INSTANCE.getJavaClassName
    val BOOLEAN = PBoolean.INSTANCE.getJavaClassName
    val VARBINARY = PVarbinary.INSTANCE.getJavaClassName
    val BINARY = PBinary.INSTANCE.getJavaClassName()
    val INTEGER_ARRAY = PIntegerArray.INSTANCE.getJavaClassName
    val UNSIGNED_INT_ARRAY = PUnsignedIntArray.INSTANCE.getJavaClassName
    val BOOLEAN_ARRAY = PBooleanArray.INSTANCE.getJavaClassName
    val VARCHAR_ARRAY = PVarcharArray.INSTANCE.getJavaClassName
    val CHAR_ARRAY = PCharArray.INSTANCE.getJavaClassName
    val VARBINARY_ARRAY = PVarbinaryArray.INSTANCE.getJavaClassName
    val BINARY_ARRAY = PBinaryArray.INSTANCE.getJavaClassName
    val LONG_ARRAY = PLongArray.INSTANCE.getJavaClassName
    val UNSIGNED_LONG_ARRAY = PUnsignedLongArray.INSTANCE.getJavaClassName
    val SMALLINT_ARRAY = PSmallintArray.INSTANCE.getJavaClassName
    val UNSIGNED_SMALLINT_ARRAY = PUnsignedSmallintArray.INSTANCE.getJavaClassName
    val TINYINT_ARRAY = PTinyintArray.INSTANCE.getJavaClassName
    val UNSIGNED_TINYINT_ARRAY = PUnsignedTinyintArray.INSTANCE.getJavaClassName
    val FLOAT_ARRAY = PFloatArray.INSTANCE.getJavaClassName
    val UNSIGNED_FLOAT_ARRAY = PUnsignedFloatArray.INSTANCE.getJavaClassName
    val DOUBLE_ARRAY = PDoubleArray.INSTANCE.getJavaClassName
    val UNSIGNED_DOUBLE_ARRAY = PUnsignedDoubleArray.INSTANCE.getJavaClassName
    val DECIMAL_ARRAY = PDecimalArray.INSTANCE.getJavaClassName
    val TIMESTAMP_ARRAY = PTimestampArray.INSTANCE.getJavaClassName
    val UNSIGNED_TIMESTAMP_ARRAY = PUnsignedTimestampArray.INSTANCE.getJavaClassName
    val DATE_ARRAY = PDateArray.INSTANCE.getJavaClassName
    val UNSIGNED_DATE_ARRAY = PUnsignedDateArray.INSTANCE.getJavaClassName
    val TIME_ARRAY = PTimeArray.INSTANCE.getJavaClassName
    val UNSIGNED_TIME_ARRAY = PUnsignedTimeArray.INSTANCE.getJavaClassName
  }

  def phoenixTypeToCatalystType(phoenixType: PDataType[_]) : DataType = {

    phoenixType.getJavaClassName match {
      case PDataTypeTable.VARCHAR | PDataTypeTable.CHAR =>
        StringType
      case PDataTypeTable.LONG | PDataTypeTable.UNSIGNED_LONG =>
        LongType
      case PDataTypeTable.INTEGER | PDataTypeTable.UNSIGNED_INT =>
        IntegerType
      case PDataTypeTable.SMALLINT | PDataTypeTable.UNSIGNED_SMALLINT =>
        ShortType
      case PDataTypeTable.TINYINT | PDataTypeTable.UNSIGNED_TINYINT =>
        ByteType
      case PDataTypeTable.FLOAT | PDataTypeTable.UNSIGNED_FLOAT =>
        FloatType
      case PDataTypeTable.DOUBLE | PDataTypeTable.UNSIGNED_DOUBLE =>
        DoubleType
      case PDataTypeTable.DECIMAL =>
        DecimalType(None)
      case PDataTypeTable.TIMESTAMP | PDataTypeTable.UNSIGNED_TIMESTAMP =>
        TimestampType
      case PDataTypeTable.TIME | PDataTypeTable.UNSIGNED_TIME =>
        TimestampType
      case PDataTypeTable.DATE | PDataTypeTable.UNSIGNED_DATE =>
        TimestampType
      case PDataTypeTable.BOOLEAN =>
        BooleanType
      case PDataTypeTable.VARBINARY | PDataTypeTable.BINARY =>
        BinaryType
      case PDataTypeTable.INTEGER_ARRAY | PDataTypeTable.UNSIGNED_INT_ARRAY =>
        ArrayType(IntegerType, containsNull = true)
      case PDataTypeTable.BOOLEAN_ARRAY =>
        ArrayType(BooleanType, containsNull = true)
      case PDataTypeTable.VARCHAR_ARRAY | PDataTypeTable.CHAR_ARRAY =>
        ArrayType(StringType, containsNull = true)
      case PDataTypeTable.VARBINARY_ARRAY | PDataTypeTable.BINARY_ARRAY =>
        ArrayType(BinaryType, containsNull = true)
      case PDataTypeTable.LONG_ARRAY | PDataTypeTable.UNSIGNED_LONG_ARRAY =>
        ArrayType(LongType, containsNull = true)
      case PDataTypeTable.SMALLINT_ARRAY | PDataTypeTable.UNSIGNED_SMALLINT_ARRAY =>
        ArrayType(IntegerType, containsNull = true)
      case PDataTypeTable.TINYINT_ARRAY | PDataTypeTable.UNSIGNED_TINYINT_ARRAY =>
        ArrayType(ByteType, containsNull = true)
      case PDataTypeTable.FLOAT_ARRAY | PDataTypeTable.UNSIGNED_FLOAT_ARRAY =>
        ArrayType(FloatType, containsNull = true)
      case PDataTypeTable.DOUBLE_ARRAY | PDataTypeTable.UNSIGNED_DOUBLE_ARRAY =>
        ArrayType(DoubleType, containsNull = true)
      case PDataTypeTable.DECIMAL_ARRAY =>
        ArrayType(DecimalType(None), containsNull = true)
      case PDataTypeTable.TIMESTAMP_ARRAY | PDataTypeTable.UNSIGNED_TIMESTAMP_ARRAY =>
        ArrayType(TimestampType, containsNull = true)
      case PDataTypeTable.DATE_ARRAY | PDataTypeTable.UNSIGNED_DATE_ARRAY =>
        ArrayType(TimestampType, containsNull = true)
      case PDataTypeTable.TIME_ARRAY | PDataTypeTable.UNSIGNED_TIME_ARRAY =>
        ArrayType(TimestampType, containsNull = true)
    }
  }
}

object PhoenixRDD {
  def NewPhoenixRDD(sc: SparkContext, table: String, columns: Seq[String],
                    predicate: Option[String] = None, conf: Configuration) = {
    new PhoenixRDD(sc, table, columns, predicate, conf)
  }
}