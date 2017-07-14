/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.plan.schema

import java.util

import com.google.common.base.Objects
import org.apache.calcite.rel.`type`._
import org.apache.calcite.sql.`type`.{MultisetSqlType, SqlTypeName}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.typeutils.MultisetTypeInfo
import org.apache.flink.table.plan.schema.MultisetRelDataType.createFieldList

import scala.collection.JavaConverters._


class MultisetRelDataType(
  val typeInfo: MultisetTypeInfo[_],
  typeFactory: FlinkTypeFactory)
  extends RelRecordType(StructKind.PEEK_FIELDS,
                        createFieldList(typeInfo.elementType.asInstanceOf[CompositeType[_]], typeFactory)) {


  override def getSqlTypeName: SqlTypeName = SqlTypeName.MULTISET

  override def toString: String = s"Multiset($typeInfo)"

  def canEqual(other: Any): Boolean = other.isInstanceOf[MapRelDataType]

  override def equals(other: Any): Boolean = other match {
    case that: MultisetRelDataType =>
      super.equals(that) &&
        (that canEqual this) &&
        typeInfo == that.typeInfo
    case _ => false
  }

  override def hashCode(): Int = {
    Objects.hashCode(typeInfo)
  }

}

object MultisetRelDataType {

//  def apply(elementTypeInfo: TypeInformation[_], typeFactory: FlinkTypeFactory): MultisetRelDataType = {
//    new MultisetRelDataType(new MultisetTypeInfo[_](elementTypeInfo.asInstanceOf[TypeInformation[_]]), typeFactory)
//  }

  private def createFieldList(
    compositeType: CompositeType[_],
    typeFactory: FlinkTypeFactory)
  : util.List[RelDataTypeField] = {

    compositeType
    .getFieldNames
    .zipWithIndex
    .map { case (name, index) =>
      new RelDataTypeFieldImpl(
        name,
        index,
        new MultisetAtomicRelDataType(
          new MultisetTypeInfo[Any](compositeType.getTypeAt(index)),
          typeFactory.createTypeFromTypeInfo(compositeType.getTypeAt(index)),
          true))
      .asInstanceOf[RelDataTypeField]
    }
    .toList
    .asJava
  }
}
