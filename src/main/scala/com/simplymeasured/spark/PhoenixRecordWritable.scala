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
import org.apache.phoenix.schema.types.{PDataType, PhoenixArray}
import scala.collection.{immutable, mutable}

class PhoenixRecordWritable extends DBWritable {
  val resultMap = mutable.Map[String, AnyRef]()

  def result : immutable.Map[String, AnyRef] = {
    resultMap.toMap
  }

  val upsertValues = mutable.ArrayBuffer[(Any, Int)]()

  override def write(statement: PreparedStatement): Unit = {
    upsertValues.zipWithIndex.map { case (s, i) => {
        if (s != null) {
          statement.setObject(i + 1, s._1, s._2)
        } else {
          statement.setNull(i + 1, s._2)
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

  def add(value: Any, sqlType: Int): Unit = {
    upsertValues.append((value, sqlType))
  }
}
