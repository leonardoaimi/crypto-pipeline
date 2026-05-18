"""
Adds the project root to sys.path so that absolute imports like
`from ingestion.kafka.schemas import ...` work in pytest without
installing the project as a package.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
