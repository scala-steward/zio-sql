package zio.sql.parser

import org.spartanz.parserz._
import zio.sql.Sql

object SelectParser extends Sql with ParserBase with App {

  import Selection._

  import this.Parser._
  import this.Parser.Expr._
  import this.Parser.Grammar._

  //TODO
  //1. handle different constant types

  private val selectKw = "select".toList.asInstanceOf[::[Char]]

  val char: G[Char] = "char" @@ consumeOption("expected: char")(
    s => s.headOption.map(s.drop(1) -> _),
    { case (s, c) => Some(s + c.toString) }
  )

  val singleQ: G[Char]  = char.filter("expected: single quote")(===(`'`))

  val kwSelect: G[::[Char]] = alpha.rep1.filter("expected keyword: SELECT")(===(selectKw))

  val string: G[String] = alpha.rep1.mapOption("string cannot be empty")(
    s => Some(s.mkString), 
    _.toList match {
      case c1 :: rest if c1.isLetter => Some(::(c1, rest))
      case _                         => None
    }
  )

  val number: G[Int] = numeric.rep1.mapOption("number cannot be empty")(
    s => Some(s.mkString.toInt), 
    _.toString.toList match {
      case c1 :: rest if c1.isDigit => Some(::(c1, rest))
      case _                         => None
    }
  )

  lazy val stringConstant: G[ColumnSelection[Any, String]] = "string constant" @@ ((singleQ, `'`) ~> string <~ (`'`, singleQ)).map(
    { case s => constant(s).value.selections._1 },
    { case c: ColumnSelection[Any, String] => c match {
        case ColumnSelection.Constant(value, None) => value 
        case ColumnSelection.Constant(value, Some(_)) => value
        case ColumnSelection.Computed(_, _) => throw new UnsupportedOperationException()
      }
    }
  )

  lazy val numberConstant: G[ColumnSelection[Any, Int]] = "number constant" @@ number.map(
    { case n => constant(n).value.selections._1
    },
    { case c: ColumnSelection[Any, Int] => c match {
        case ColumnSelection.Constant(value, None) => value
        case ColumnSelection.Constant(value, Some(_)) => value
        case ColumnSelection.Computed(_, _) => throw new UnsupportedOperationException()
      }
    }
  )

  val selections = "selections" @@ (stringConstant | numberConstant).separated(comma)

  val select: G[Read.Select[Any,SelectionSet[Any]]] = "select" @@ (kwSelect ~ space ~ selections).map(
    { case ((_, _), sel) => select(Selection[Any,SelectionSet[Any]](sel.values.reverse.foldLeft[SelectionSet[Any]](SelectionSet.Empty) { case (app, n) => n.merge :*: app })).from(Table.Empty) },
    { case t => ((selectKw, ` `), SeparatedBy.fromList(t.selection.value.selectionsUntyped.map { s => s match {
                                                                  case a@ColumnSelection.Constant(v, _) => if (v.isInstanceOf[String]) {
                                                                    Left(a.asInstanceOf[ColumnSelection.Constant[String]])
                                                                  } else Right(a.asInstanceOf[ColumnSelection.Constant[Int]])
                                                                  case _@ColumnSelection.Computed(_, _) => throw new UnsupportedOperationException()
                                                                }}, `,`)) }
  )

  val parser: (Unit, Input) => (Unit, String \/ (Input, Read.Select[Any, SelectionSet[Any]]))  = Parser.parser(select)
  val printer: (Unit, (Input, Read.Select[Any, SelectionSet[Any]])) => (Unit, String \/ Input) = Parser.printer(select)
  val description: List[String]                                  = Parser.bnf(select)

  //testfield
  {
    val a = constant(1) ++ constant("foo") ++ constant(true)
    val c = a.value.selectionsUntyped
    val d = c.foldLeft[SelectionSet[Any]](SelectionSet.Empty) { case (app, n) => n :*: app }
    val e: Selection[Any,SelectionSet[Any]] = Selection(d)
    val _ = select(e).from(Table.Empty)
  }
  
  //example from string
  {
    val inputString = "select 1,'foo','bar'"
    val expectedStruct = select(constant(1) ++ constant("foo") ++ constant("bar")).from(Table.Empty)
    val expectedString = "select 1,'foo','bar'"
  
    val Right((_, r1)) = parser((), inputString)._2
    println("struct ok: " + (r1 == expectedStruct))
    println("expected: " + expectedStruct)
    println("output:   " + r1)
    val (_, Right(r2)) = printer((), ("", r1))
    println("string ok: " + (r2 == expectedString))
    println("expected: " + expectedString)
    println("output:   " + r2)
  }

  //example from model
  {
    val columnsSel = 
      //computed(Expr.FunctionCall(Expr.Literal("test"), FunctionDef.CharLength)) ++ 
      constant(1) ++ constant("foo") ++ constant("bar")/* ++ constant(true)*/
    val inputStruct = select(columnsSel).from(Table.Empty)
    val expectedString = "select 1,'foo','bar'"

    val (_, Right(p)) = printer((), ("", inputStruct))
    println("string ok: " + (p == expectedString))
    println("expected: " + expectedString)
    println("output: " + p)
  }

}
