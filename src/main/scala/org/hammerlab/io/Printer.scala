package org.hammerlab.io

import java.io.PrintStream

case class Printer(ps: PrintStream) {
  def print(os: Object*): Unit =
    for { o ← os } {
      ps.println(o)
    }

  def printList(list: Seq[_],
                header: String,
                truncatedHeader: Int ⇒ String,
                limit: Option[Int] = None,
                indent: String = "\t"): Unit =
    limit match {
      case Some(splitsPrintLimit)
        if list.length > splitsPrintLimit ⇒
        print(
          truncatedHeader(splitsPrintLimit),
          list
            .take(splitsPrintLimit)
            .mkString("\t", "\n\t", ""),
          "…"
        )
      case Some(splitsPrintLimit)
        if splitsPrintLimit == 0 ⇒
      // No-op
      case _ ⇒
        print(
          header,
          list.mkString("\t", "\n\t", "")
        )
    }

}

object Printer {

  implicit def makePrinter(ps: PrintStream): Printer = Printer(ps)
  implicit def unmakePrinter(p: Printer): PrintStream = p.ps

  def apply(file: Option[String]): Printer =
    file match {
      case Some(file) ⇒
        new PrintStream(file)
      case None ⇒
        System.out
    }

  def print(os: Object*)(
      implicit printer: Printer
  ): Unit =
    printer.print(os: _*)

  def printList(list: Seq[_],
                header: String,
                truncatedHeader: Int ⇒ String,
                limit: Option[Int] = None,
                indent: String = "\t")(
                   implicit printer: Printer
               ): Unit =
    printer.printList(
      list,
      header,
      truncatedHeader,
      limit,
      indent
    )
}
