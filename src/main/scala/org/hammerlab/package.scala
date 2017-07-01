package org

import caseapp.core.{ ContextParser, Messages, Parser, WithHelp }
import org.hammerlab.hadoop.Configuration
import org.hammerlab.spark.{ Conf, Context }

import scala.reflect.ClassTag
import scala.sys.exit

package object hammerlab {

  abstract class ContextApp[Ctx, Args](implicit
                                       ct: ClassTag[Args],
                                       contextParser: ContextParser[Ctx, Args]) {
    implicit val sc: Ctx

    def run(args: Args,
            remainingArgs: Seq[String]): Unit

    implicit lazy val parser: Parser[Args] = contextParser.apply

    implicit lazy val messages: Messages[Args] = implicitly[Messages[Args]]

    def main(args: Array[String]): Unit = {

      def error(message: String): Unit = {
        Console.err.println(message)
        exit(1)
      }

      def helpAsked(): Unit = {
        println(messages.withHelp.helpMessage)
        exit(0)
      }

      def usageAsked(): Unit = {
        println(messages.withHelp.usageMessage)
        exit(0)
      }

      parser.withHelp.detailedParse(args) match {
        case Left(err) ⇒
          error(err)

        case Right((WithHelp(usage, help, args), remainingArgs, extraArgs)) ⇒

          if (help)
            helpAsked()

          if (usage)
            usageAsked()

          run(args, remainingArgs)
      }
    }
  }

  abstract class SparkApp[Args](implicit
                                ct: ClassTag[Args],
                                contextParser: ContextParser[Context, Args])
    extends ContextApp[Context, Args] {
    implicit val sparkConf = Conf()
    override implicit val sc: Context = Context()
  }

  abstract class HadoopApp[Args](implicit
                                 ct: ClassTag[Args],
                                 contextParser: ContextParser[Configuration, Args])
    extends ContextApp[Configuration, Args] {
    override implicit val sc: Configuration = Configuration()
  }

}
