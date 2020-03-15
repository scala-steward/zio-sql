package zio.sql

import org.spartanz.parserz._

object SelectParser extends Sql with App {

  import Selection._

  object Parser extends ParsersModule {
    override type Input = String
  }

  import Parser._
  import Parser.Expr._
  import Parser.Grammar._

  type S    = Unit
  type E    = String
  type G[A] = Grammar[S, S, E, A]

  //TODO
  //1. handle different constant types

  private val `(` = '('
  private val `)` = ')'
  private val `,` = ','
  private val ` ` = ' '
  private val `'` = '\''
  private val selectKw = "select".toList.asInstanceOf[::[Char]]

  val char: G[Char] = "char" @@ consumeOption("expected: char")(
    s => s.headOption.map(s.drop(1) -> _),
    { case (s, c) => Some(s + c.toString) }
  )

  val alpha: G[Char]    = char.filter("expected: alphabetical")(cond(_.isLetter)).tag("alpha")
  val comma: G[Char]    = char.filter("expected: comma")(===(`,`))
  val paren1: G[Char]   = char.filter("expected: open paren")(===(`(`))
  val paren2: G[Char]   = char.filter("expected: close paren")(===(`)`))
  val singleQ: G[Char]  = char.filter("expected: single quote")(===(`'`))
  val space: G[Char]    = char.filter("expected: space")(===(` `))
  val kwSelect: G[::[Char]] = alpha.rep1.filter("expected keyword: SELECT")(===(selectKw))
  val whitespace: G[Char] = char.filter("expected: whitespace")(cond(_.isWhitespace))
  val whitespaces: G[::[Char]] = whitespace.rep1

  val whitespaceComma = "whitespace comma" @@ whitespace.rep ~ comma ~ whitespace.rep

  val stringConstant: G[String] = "constant" @@ alpha.rep1.mapOption("constant cannot be empty")(
    s => Some(s.mkString), 
    _.toList match {
      case c1 :: rest if c1.isLetter => Some(::(c1, rest))
      case _                         => None
    }
  )

  lazy val selection: G[ColumnSelection[Any, _]] = "selection" @@ (singleQ ~ stringConstant ~ singleQ).map(
    { case ((_, s), _) => constant(s).value.selectionsUntyped.head
    },
    { case c: ColumnSelection[_, _] => c match {
        case ColumnSelection.Constant(value, None) => ((`'`, value.toString()), `'`) 
        case ColumnSelection.Constant(value, Some(_)) => ((`'`, value.toString()), `'`) 
        case ColumnSelection.Computed(_, _) => throw new UnsupportedOperationException("Computed values are unsopported")
      }
    }
  )

  val selections = "selections" @@ selection.separated(whitespaceComma)

  val select: G[Read.Select[Any,SelectionSet[Any]]] = "select" @@ (kwSelect ~ whitespaces ~ selections).map(
    { case ((_, _), sel) => select(Selection[Any,SelectionSet[Any]](sel.values.reverse.foldLeft[SelectionSet[Any]](SelectionSet.Empty) { case (app, n) => n :*: app })).from(Table.Empty) },
    { case t => ((selectKw, ::(` `, Nil)), SeparatedBy.fromList(t.selection.value.selectionsUntyped, ((Nil, `,`), List(` `)))) }
  )

  val parser: (Unit, Input) => (Unit, String \/ (Input, Read.Select[Any, SelectionSet[Any]]))  = Parser.parser(select)
  val printer: (Unit, (Input, Read.Select[Any, SelectionSet[Any]])) => (Unit, String \/ Input) = Parser.printer(select)
  val description: List[String]                                  = Parser.bnf(select)

  //testfield
  val a = constant(1) ++ constant("foo") ++ constant(true)
  val c = a.value.selectionsUntyped
  val d = c.foldLeft[SelectionSet[Any]](SelectionSet.Empty) { case (app, n) => n :*: app }
  val e: Selection[Any,SelectionSet[Any]] = Selection(d)
  val f = select(e).from(Table.Empty)

  
  //example from string
  {
    val inputString = """select  
      'foo', 'bar'"""
    val expectedStruct = select(constant("foo") ++ constant("bar")).from(Table.Empty)
    val expectedString = "select 'foo', 'bar'"
  
    val Right((_, r1)) = parser((), inputString)._2
    println("struct ok: " + (r1 == expectedStruct))
    println("expected: " + expectedStruct)
    println("output: " + r1)
    val (_, Right(r2)) = printer((), ("", r1))
    println("string ok: " + (r2 == expectedString))
    println("expected: " + expectedString)
    println("output: " + r2)
  }

  //example from model
  {
    val columnsSel = 
      //computed(Expr.FunctionCall(Expr.Literal("test"), FunctionDef.CharLength)) ++ 
      /*constant(1) ++ */constant("foo") ++ constant("bar")/* ++ constant(true)*/
    val inputStruct = select(columnsSel).from(Table.Empty)
    val expectedString = "select 'foo', 'bar'"

    val (_, Right(p)) = printer((), ("", inputStruct))
    println("string ok: " + (p == expectedString))
    println("expected: " + expectedString)
    println("output: " + p)
  }

}
