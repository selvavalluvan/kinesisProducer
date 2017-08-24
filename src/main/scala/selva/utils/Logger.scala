package selva.utils

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator

object Logger {

    Configurator.setLevel("org", Level.OFF)
    Configurator.setLevel("com.amazonaws", Level.INFO)
    Configurator.setLevel("akka", Level.OFF)

    def getInstance(name: String) = LogManager.getLogger(name)
}


