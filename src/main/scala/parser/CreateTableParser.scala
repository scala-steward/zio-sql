package zio.sql.parser

import com.github.ghik.silencer.silent
import org.spartanz.parserz._
import zio.sql.Sql

object CreateTableParser extends Sql with ParserBase with App {

  import Column._
  
  val columnsString = "id int,name varchar,legendary boolean"
  val tableString   = s"create table pokemons($columnsString)"

  val columnsStmt = int("id") :*: string("name") :*: boolean("legendary") :*: ColumnSet.Empty
  val table = columnsStmt.table("pokemons")

  import this.Parser._
  import this.Parser.Expr._
  import this.Parser.Grammar._

  val createKw = "create".toList.asInstanceOf[::[Char]]
  val tableKw  = "table".toList.asInstanceOf[::[Char]]

  val kwCreate: G[::[Char]] = alpha.rep1.filter("expected keyword: CREATE")(===(createKw))
  val kwTable: G[::[Char]] =  alpha.rep1.filter("expected keyword: TABLE")(===(tableKw))

  //TODO
  //1. handle empty column set
  //2. handle uppercase/lowercase
  //3. handle digits in column types: int2, int4, int8, etc.
  //4. pretty print the resulting string

  val name: G[String] = "name" @@ alpha.rep1.mapOption("name cannot be empty")(
    s => Some(s.mkString), 
    _.toList match {
      case c1 :: rest if c1.isLetter => Some(::(c1, rest))
      case _                         => None
    }
  )

  val typee: G[String] = "type" @@ alpha.rep1.mapOption("type cannot be empty")(
    s => Some(s.mkString),
    _.toList match {
      case c1 :: rest if c1.isLetter => Some(::(c1, rest))
      case _                         => None
    }
  )

  val column: G[Column[_]] = "column" @@ (name ~ space ~ typee).map(
    { case ((n, _), t) => t.toLowerCase match {
                            case "bigint" | "int8" => Column.long(n)
                            case "boolean"         => Column.boolean(n)
                            case "int" | "int4"    => Column.int(n)
                            case "varchar"         => Column.string(n)
                          }
    },
    { case c@Column(n) => ((n, ` `), c.typeTag match {
                            case TypeTag.TBigDecimal     => "decimal"
                            case TypeTag.TBoolean        => "boolean"
                            case TypeTag.TDouble         => "double precision"
                            case TypeTag.TFloat          => "float"
                            case TypeTag.TInstant        => "timestamp"
                            case TypeTag.TInt            => "int"
                            case TypeTag.TLocalDate      => "date"
                            case TypeTag.TLocalDateTime  => "timestamp"
                            case TypeTag.TLocalTime      => "time"
                            case TypeTag.TLong           => "bigint"
                            case TypeTag.TOffsetDateTime => "timestamp with timezone"
                            case TypeTag.TOffsetTime     => "time with timezone"
                            case TypeTag.TShort          => "smallint"
                            case TypeTag.TString         => "varchar"
                          })
    }
  )

  val columns = "columns" @@ column.separated(comma)

  lazy val columnSet: G[ColumnSet] = "columnSet" @@ columns.map({
    case e => e.values.foldLeft[ColumnSet](ColumnSet.Empty) { case (a, el) => el :*: a }
  }, {
    case e => SeparatedBy.fromList(e.columnsUntyped, `,`)
  })

  //change Table to Table.Source and get rid of the ColumnSet.Empty case
  //to consider: PostgreSQL allows creating tables with no columns for partitioning, 
  //  but this requires a change to Table model. Probably not something ZIO SQL needs to support.
  @silent
  val tab: G[Table] = "table" @@ (kwCreate ~ space ~ kwTable ~ space ~ name ~ ((paren1, `(`) ~> columnSet <~ (`)`, paren2))).map(
    { case ((a, name), exp) => exp match {
                                  case ColumnSet.Empty => throw new IllegalArgumentException("Column set can't be empty")
                                  case c: ColumnSet.Cons[_, _] => c.table(name)
                                } },
    { case t => t match {
                  case ts: Table.Source[_, _] => (((((createKw, ` `), tableKw), ` `), ts.name),ts.columnsUntyped.reverse.foldLeft[ColumnSet](ColumnSet.Empty) { case (a, c) => c :*: a })
                  case _ => throw new IllegalArgumentException("Column set can't be empty")
                } }
  )

  val parser: (Unit, Input) => (Unit, String \/ (Input, Table))  = Parser.parser(tab)
  val printer: (Unit, (Input, Table)) => (Unit, String \/ Input) = Parser.printer(tab)
  val description: List[String]                                  = Parser.bnf(tab)

  //example
  @silent
  val Right((_, r)) = parser((), tableString)._2

  @silent
  val (_, Right(p)) = printer((), ("", table))

  println(r == table) //false, why?
  println(p == tableString)
  println(r)
  println(p)
}
