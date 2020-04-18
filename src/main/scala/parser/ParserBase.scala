package zio.sql.parser

import com.github.ghik.silencer.silent
import org.spartanz.parserz._

trait ParserBase extends Sql {

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
  //1. handle empty column set
  //2. handle uppercase/lowercase
  //3. handle digits in column types: int2, int4, int8, etc.
  //4. pretty print the resulting string

  val `(` = '('
  val `)` = ')'
  val `,` = ','
  val ` ` = ' '
  val `'` = '\''

  val char: G[Char] = "char" @@ consumeOption("expected: char")(
    s => s.headOption.map(s.drop(1) -> _),
    { case (s, c) => Some(s + c.toString) }
  )

  val alpha: G[Char]    = char.filter("expected: alphabetical")(cond(_.isLetter)).tag("alpha")
  val numeric: G[Char]  = char.filter("expected: numeric")(cond(_.isDigit)).tag("numeric")
  val comma: G[Char]    = char.filter("expected: comma")(===(`,`))
  val paren1: G[Char]   = char.filter("expected: open paren")(===(`(`))
  val paren2: G[Char]   = char.filter("expected: close paren")(===(`)`))
  val space: G[Char]    = char.filter("expected: space")(===(` `))

}
