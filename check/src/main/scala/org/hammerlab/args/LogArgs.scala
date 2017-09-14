package org.hammerlab.args

import caseapp.{ HelpMessage ⇒ M }

case class LogArgs(
    @M("Set the root logging level to WARN; useful for making Spark display the console progress-bar in client-mode")
    warn: Boolean = false
)
