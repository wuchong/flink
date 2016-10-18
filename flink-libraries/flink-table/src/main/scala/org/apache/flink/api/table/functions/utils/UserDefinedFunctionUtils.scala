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


package org.apache.flink.api.table.functions.utils

import java.lang.reflect.Method
import java.sql.{Date, Time, Timestamp}

import com.google.common.primitives.Primitives
import org.apache.flink.api.common.functions.InvalidTypesException
import org.apache.flink.api.common.typeinfo.{AtomicType, TypeInformation}
import org.apache.flink.api.java.typeutils.{PojoTypeInfo, TupleTypeInfo, TypeExtractor}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.api.table.{TableException, ValidationException}
import org.apache.flink.api.table.functions.{TableFunction, ScalarFunction, EvaluableFunction,
UserDefinedFunction}
import org.apache.flink.util.InstantiationUtil

object UserDefinedFunctionUtils {

  /**
    * Instantiates a user-defined function.
    */
  def instantiate[T <: UserDefinedFunction](clazz: Class[T]): T = {
    val constructor = clazz.getDeclaredConstructor()
    constructor.setAccessible(true)
    constructor.newInstance()
  }

  /**
    * Checks if a user-defined function can be easily instantiated.
    */
  def checkForInstantiation(clazz: Class[_]): Unit = {
    if (!InstantiationUtil.isPublic(clazz)) {
      throw ValidationException("Function class is not public.")
    }
    else if (!InstantiationUtil.isProperClass(clazz)) {
      throw ValidationException("Function class is no proper class, it is either abstract," +
        " an interface, or a primitive type.")
    }
    else if (InstantiationUtil.isNonStaticInnerClass(clazz)) {
      throw ValidationException("The class is an inner class, but not statically accessible.")
    }

    // check for default constructor (can be private)
    clazz
      .getDeclaredConstructors
      .find(_.getParameterTypes.isEmpty)
      .getOrElse(throw ValidationException("Function class needs a default constructor."))
  }

  // ----------------------------------------------------------------------------------------------
  // Utilities for EvaluableFunction
  // ----------------------------------------------------------------------------------------------

  /**
    * Prints one signature consisting of classes.
    */
  def signatureToString(signature: Array[Class[_]]): String =
    "(" + signature.map { clazz =>
      if (clazz == null) {
        "null"
      } else {
        clazz.getCanonicalName
      }
    }.mkString(", ") + ")"

  /**
    * Prints one signature consisting of TypeInformation.
    */
  def signatureToString(signature: Seq[TypeInformation[_]]): String = {
    signatureToString(typeInfoToClass(signature))
  }

  /**
    * Extracts type classes of [[TypeInformation]] in a null-aware way.
    */
  def typeInfoToClass(typeInfos: Seq[TypeInformation[_]]): Array[Class[_]] =
    typeInfos.map { typeInfo =>
      if (typeInfo == null) {
        null
      } else {
        typeInfo.getTypeClass
      }
    }.toArray


  /**
    * Compares parameter candidate classes with expected classes. If true, the parameters match.
    * Candidate can be null (acts as a wildcard).
    */
  def parameterTypeEquals(candidate: Class[_], expected: Class[_]): Boolean =
    candidate == null ||
      candidate == expected ||
      expected.isPrimitive && Primitives.wrap(expected) == candidate ||
      candidate == classOf[Date] && expected == classOf[Int] ||
      candidate == classOf[Time] && expected == classOf[Int] ||
      candidate == classOf[Timestamp] && expected == classOf[Long]

  /**
    * Returns signatures matching the given signature of [[TypeInformation]].
    * Elements of the signature can be null (act as a wildcard).
    */
  def getSignature(
      function: EvaluableFunction,
      signature: Seq[TypeInformation[_]])
    : Option[Array[Class[_]]] = {
    // We compare the raw Java classes not the TypeInformation.
    // TypeInformation does not matter during runtime (e.g. within a MapFunction).
    val actualSignature = typeInfoToClass(signature)

    function
      .getSignatures
      // go over all signatures and find one matching actual signature
      .find { curSig =>
        // match parameters of signature to actual parameters
        actualSignature.length == curSig.length &&
          curSig.zipWithIndex.forall { case (clazz, i) =>
            parameterTypeEquals(actualSignature(i), clazz)
          }
      }
  }

  /**
    * Returns eval method matching the given signature of [[TypeInformation]].
    */
  def getEvalMethod(
    function: EvaluableFunction,
    signature: Seq[TypeInformation[_]])
  : Option[Method] = {
    // We compare the raw Java classes not the TypeInformation.
    // TypeInformation does not matter during runtime (e.g. within a MapFunction).
    val actualSignature = typeInfoToClass(signature)

    function
      .getEvalMethods
      // go over all eval methods and find one matching
      .find { cur =>
        val signatures = cur.getParameterTypes
        // match parameters of signature to actual parameters
        actualSignature.length == signatures.length &&
          signatures.zipWithIndex.forall { case (clazz, i) =>
            parameterTypeEquals(actualSignature(i), clazz)
          }
      }
  }



  /**
    * Internal method of [[ScalarFunction#getResultType()]] that does some pre-checking and uses
    * [[TypeExtractor]] as default return type inference.
    */
  def getResultType(
      scalarFunction: ScalarFunction,
      signature: Array[Class[_]])
    : TypeInformation[_] = {
    // find method for signature
    val evalMethod = scalarFunction.getEvalMethods
      .find(m => signature.sameElements(m.getParameterTypes))
      .getOrElse(throw new ValidationException("Given signature is invalid."))

    val userDefinedTypeInfo = scalarFunction.getResultType(signature)
    if (userDefinedTypeInfo != null) {
        userDefinedTypeInfo
    } else {
      try {
        TypeExtractor.getForClass(evalMethod.getReturnType)
      } catch {
        case ite: InvalidTypesException =>
          throw new ValidationException(s"Return type of scalar function '$this' cannot be " +
            s"automatically determined. Please provide type information manually.")
      }
    }
  }

  /**
    * Internal method of [[ScalarFunction#getResultType()]] that does some pre-checking and uses
    * [[TypeExtractor]] as default return type inference.
    */
  def getResultType(
    tableFunction: TableFunction[_],
    signature: Array[Class[_]])
  : TypeInformation[_] = {
    // find method for signature
    val evalMethod = tableFunction.getEvalMethods
      .find(m => signature.sameElements(m.getParameterTypes))
      .getOrElse(throw new ValidationException("Given signature is invalid."))

    val userDefinedTypeInfo = tableFunction.getResultType
    if (userDefinedTypeInfo != null) {
      userDefinedTypeInfo
    } else {
      try {
        TypeExtractor.getForClass(evalMethod.getReturnType)
      } catch {
        case ite: InvalidTypesException =>
          throw new ValidationException(
            s"Return type of table function '$this' cannot be " +
              s"automatically determined. Please provide type information manually.")
      }
    }
  }

  /**
    * Returns the return type of the evaluation method matching the given signature.
    */
  def getResultTypeClass(
      function: EvaluableFunction,
      signature: Array[Class[_]])
    : Class[_] = {
    // find method for signature
    val evalMethod = function.getEvalMethods
      .find(m => signature.sameElements(m.getParameterTypes))
      .getOrElse(throw new IllegalArgumentException("Given signature is invalid."))
    evalMethod.getReturnType
  }

  /**
    * Prints all signatures of a [[EvaluableFunction]].
    */
  def signaturesToString(function: EvaluableFunction): String = {
    function.getSignatures.map(signatureToString).mkString(", ")
  }

  /**
    * Returns field names and field positions for a given [[TypeInformation]].
    *
    * Field names are automatically extracted for
    * [[org.apache.flink.api.common.typeutils.CompositeType]].
    *
    * @param inputType The TypeInformation extract the field names and positions from.
    * @return A tuple of two arrays holding the field names and corresponding field positions.
    */
  def getFieldInfo(inputType: TypeInformation[_])
  : (Array[String], Array[Int]) = {
    val fieldNames: Array[String] = inputType match {
      case t: TupleTypeInfo[_] => t.getFieldNames
      case c: CaseClassTypeInfo[_] => c.getFieldNames
      case p: PojoTypeInfo[_] => p.getFieldNames
      case a: AtomicType[_] => Array("f0")
      case tpe =>
        throw new TableException(s"Type $tpe lacks explicit field naming")
    }
    val fieldIndexes = fieldNames.indices.toArray
    (fieldNames, fieldIndexes)
  }

  /**
    * Returns field names and field types for a given [[TypeInformation]].
    *
    * Field names are automatically extracted for
    * [[org.apache.flink.api.common.typeutils.CompositeType]].
    *
    * @param inputType The TypeInformation extract the field names and types from.
    * @tparam A The type of the TypeInformation.
    * @return A tuple of two arrays holding the field names and corresponding field types.
    */
  def getFieldAttribute[A](inputType: TypeInformation[A])
  : (Array[String], Array[TypeInformation[_]]) = {
    val fieldNames: Array[String] = inputType match {
      case t: TupleTypeInfo[A] => t.getFieldNames
      case c: CaseClassTypeInfo[A] => c.getFieldNames
      case p: PojoTypeInfo[A] => p.getFieldNames
      case a: AtomicType[A] => Array("f0")
      case tpe =>
        throw new TableException(s"Type $tpe lacks explicit field naming")
    }
    val fieldTypes: Array[TypeInformation[_]] = fieldNames.map { i =>
      inputType match {
        case t: TupleTypeInfo[A] => t.getTypeAt(i).asInstanceOf[TypeInformation[_]]
        case c: CaseClassTypeInfo[A] => c.getTypeAt(i).asInstanceOf[TypeInformation[_]]
        case p: PojoTypeInfo[A] => p.getTypeAt(i).asInstanceOf[TypeInformation[_]]
        case a: AtomicType[A] => a.asInstanceOf[TypeInformation[_]]
        case tpe =>
          throw new TableException(s"Type $tpe lacks explicit field naming")
      }
    }
    (fieldNames, fieldTypes)
  }
}
