package com.cdh.kafkaStreaming.exception

final case class KafkaStreamException(
  private val message: String = "",
  private val cause: Throwable = None.orNull)
  extends Exception(message, cause)