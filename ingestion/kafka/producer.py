"""
Kafka producer wrapper used by all ingestion sources to serialise and publish
events to the appropriate topics. Handles retries and delivery confirmation.
"""
