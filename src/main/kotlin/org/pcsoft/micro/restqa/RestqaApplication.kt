package org.pcsoft.micro.restqa

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication

/**
 * Spring Boot application class for RESTAQ — a lightweight REST-to-queue messaging gateway.
 *
 * Enables component scanning and configuration-property binding for all `restqa.*` properties.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
class RestqaApplication

/**
 * Application entry point. Bootstraps the Spring Boot context for RESTAQ.
 */
fun main(args: Array<String>) {
	runApplication<RestqaApplication>(*args)
}
