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

import java.sql.{PreparedStatement, ResultSet}
import org.apache.hadoop.mapreduce.lib.db.DBWritable
import org.apache.phoenix.mapreduce.util.ColumnInfoToStringEncoderDecoder
import org.apache.phoenix.schema.types.{PDataType, PhoenixArray}
import scala.collection.{immutable, mutable}
import scala.collection.JavaConversions._

class PhoenixRecordWritable(var encodedColumns: String) extends DBWritable {
  val upsertValues = mutable.ArrayBuffer[Any]()

  val resultMap = mutable.Map[String, AnyRef]()

  def result : immutable.Map[String, AnyRef] = {
    resultMap.toMap
  }

  override def write(statement: PreparedStatement): Unit = {
    // Decode the ColumnInfo list
    val columns = ColumnInfoToStringEncoderDecoder.decode(encodedColumns).toList

    // Make sure we at least line up in size
    if(upsertValues.length != columns.length) {
      throw new UnsupportedOperationException(s"Number of fields ($upsertValues.length) does not equal column size ($columns.length)")
    }

    // Each value (v) corresponds to a column type (c) and an index (i)
    upsertValues.zip(columns).zipWithIndex.map { case ((v, c), i) => {
        if (v != null) {
          statement.setObject(i + 1, v, c.getSqlType)
        } else {
          statement.setNull(i + 1, c.getSqlType)
        }
      }
    }
  }

  override def readFields(resultSet: ResultSet): Unit = {
    val metadata = resultSet.getMetaData
    for(i <- 1 to metadata.getColumnCount) {

      val value = resultSet.getObject(i) match {
        case x: PhoenixArray => x.getArray
        case y => y
      }

      resultMap(metadata.getColumnLabel(i)) = value
    }
  }

  def add(value: Any): Unit = {
    upsertValues.append(value)
  }

  // Empty constructor to satisfy hadoop reflection
  def this() = {
    this("")
  }

  def setEncodedColumns(encodedColumns: String) {
    this.encodedColumns = encodedColumns
  }
}
