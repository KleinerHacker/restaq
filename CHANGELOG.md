# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

# [UNRELEASED]

### Added

- REST → Queue gateway (sender) with AMQP and JMS support
- Queue → REST consumer delivery (receiver) with HTTP callback
- Configurable retry with backoff and dead-letter queue support
- Message time-to-live enforcement
- RFC 9457 Problem Details error responses
- Transparent HTTP header propagation with transport/TLS header filtering
- Configurable payload size limits
- Multi-sender and multi-receiver configuration
