package main.transformers

import scala.meta._

class LiteralTransformer extends Transformer {
  var in_func = false
  var param : List[Term.Param] = null

  override def apply(tree: Tree): Tree = tree match {

    case q"$expr.$ename((..$params) => $expr2)" => {
      param = params
      if(ename.toString() == "map")
        in_func = true

      var expr2New = super.apply(expr2).asInstanceOf[Term]

      in_func = false
      q"$expr.$ename((..$params) => $expr2New)"
    }

    case Lit.Int(literal) => {
      if(in_func) {
        var x = Term.Name(param.head.toString().stripPrefix("(").stripSuffix(")"))
        return q"new SymInt($literal, $x.getProvenance())"
      }
      q"literal"
    }

    case node => {
      super.apply(node)
    }
  }
}
