import pytest
from pydantic import ValidationError

from pipeline.config import Settings


def test_defaults_are_consistent():
    Settings()


def test_lease_must_cover_the_worst_case_attempt():
    """claim (db timeout) + processing timeout + complete (db timeout) must fit in the lease."""
    with pytest.raises(ValidationError, match="lease"):
        Settings(lease_s=15, processing_timeout_s=10, db_command_timeout_s=5)

    Settings(lease_s=21, processing_timeout_s=10, db_command_timeout_s=5)
