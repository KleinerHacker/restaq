package org.pcsoft.micro.restqa

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication

@SpringBootApplication
@ConfigurationPropertiesScan
class RestqaApplication

fun main(args: Array<String>) {
	runApplication<RestqaApplication>(*args)
}
