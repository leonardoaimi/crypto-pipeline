"""
Validates raw Kafka messages against their Pydantic schemas before they are
forwarded downstream. Messages that fail validation are routed to the
crypto.trades.dlq (dead-letter queue) topic.
"""
