package org.pcsoft.micro.restqa

import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.jms.core.JmsTemplate
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.bean.override.mockito.MockitoBean

/**
 * Verifies that the Spring context starts cleanly for **both** queue variants with a
 * full sender/receiver configuration, without ever talking to a real broker. The
 * broker-connecting beans are replaced by Mockito mocks via [MockitoBean], so the
 * queue declarations (AMQP) and template/validation setup (JMS) run without a broker.
 */
class RestqaApplicationTests {

    /**
     * Tests that the Spring application context loads successfully with the AMQP profile
     * active and a full sender/receiver configuration. Broker-connecting beans are mocked
     * to avoid requiring a real RabbitMQ instance.
     */
    @Nested
    @SpringBootTest(useMainMethod = SpringBootTest.UseMainMethod.ALWAYS)
    @ActiveProfiles("amqp")
    inner class AmqpContext {

        @MockitoBean
        private lateinit var connectionFactory: org.springframework.amqp.rabbit.connection.ConnectionFactory

        @MockitoBean
        private lateinit var rabbitTemplate: org.springframework.amqp.rabbit.core.RabbitTemplate

        @MockitoBean
        private lateinit var amqpAdmin: org.springframework.amqp.core.AmqpAdmin

        @MockitoBean
        private lateinit var amqpQueueConsumer: org.pcsoft.micro.restqa.receive.port.AmqpQueueConsumer

        /**
         * Verifies that the Spring application context starts cleanly with the AMQP
         * profile active, all required beans are wired, and no configuration errors occur
         * when broker beans are replaced by mocks.
         */
        @Test
        fun contextLoads() {
        }
    }

    /**
     * Tests that the Spring application context loads successfully with the JMS profile
     * active and a full sender/receiver configuration. Broker-connecting beans are mocked
     * and JmsAutoConfiguration is excluded to avoid requiring a real ActiveMQ instance.
     */
    @Nested
    @SpringBootTest(
        // The mocked JmsTemplate has no message converter, which Boot's
        // jmsMessagingTemplate auto-config rejects; excluding it keeps the mock clean.
        properties = ["spring.autoconfigure.exclude=org.springframework.boot.jms.autoconfigure.JmsAutoConfiguration"],
        useMainMethod = SpringBootTest.UseMainMethod.ALWAYS
    )
    @ActiveProfiles("jms")
    inner class JmsContext {

        @MockitoBean
        private lateinit var connectionFactory: jakarta.jms.ConnectionFactory

        @MockitoBean
        private lateinit var jmsTemplate: JmsTemplate

        @MockitoBean
        private lateinit var jmsQueueConsumer: org.pcsoft.micro.restqa.receive.port.JmsQueueConsumer

        /**
         * Verifies that the Spring application context starts cleanly with the JMS
         * profile active, all required beans are wired, and no configuration errors occur
         * when broker beans are replaced by mocks and JmsAutoConfiguration is excluded.
         */
        @Test
        fun contextLoads() {
        }
    }
}
