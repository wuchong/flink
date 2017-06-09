package org.apache.flink.table.codegen.calls

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.{CodeGenerator, GeneratedExpression}

class PrevGen(returnType: TypeInformation[_]) extends CallGenerator {

  override def generate(
    codeGenerator: CodeGenerator,
    operands: Seq[GeneratedExpression])
  : GeneratedExpression = {
    val resultTerm = newName("result")
    val nullTerm = newName("isNull")
    val resultTypeTerm = primitiveTypeTermForTypeInfo(returnType)
    val resultCode =
      s"""
        |boolean $nullTerm = false;
        |$resultTypeTerm $resultTerm;
        |
        |${operands(1).code}
        |if (${operands(1).resultTerm} == 0) {
        |  $resultTerm = ${codeGenerator.input1Term};
        |} else {
        |  $resultTerm = 10;
        |}
        |""".stripMargin
    GeneratedExpression(resultTerm, nullTerm, resultCode, returnType)
  }
}
