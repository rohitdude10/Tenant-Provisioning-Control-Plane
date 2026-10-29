from datetime import UTC

import pytest
from pydantic import ValidationError

from app.core.time import format_timestamp
from app.schemas import TenantCreate, TenantPatch


@pytest.mark.parametrize("slug", ["ab", "Upper", "1abc", "ab-", "bad_slug", "x" * 29])
def test_tenant_slug_rejects_invalid_values(slug):
    with pytest.raises(ValidationError):
        TenantCreate(slug=slug, name="Example")


@pytest.mark.parametrize("slug", ["abc", "a-b", "a1b", "a" + "b" * 26 + "9"])
def test_tenant_slug_accepts_valid_values(slug):
    assert TenantCreate(slug=slug, name="Example").slug == slug


def test_tenant_name_is_trimmed_and_cannot_be_blank():
    assert TenantCreate(slug="abc", name=" Example ").name == "Example"
    with pytest.raises(ValidationError):
        TenantCreate(slug="abc", name="   ")


def test_patch_requires_expected_version_and_mutable_data():
    assert TenantPatch(version=2, name="Updated").name == "Updated"
    with pytest.raises(ValidationError):
        TenantPatch(version=2)


def test_timestamp_serialization_has_utc_milliseconds_and_z_suffix():
    from datetime import datetime

    assert format_timestamp(datetime(2025, 1, 2, 3, 4, 5, 123456, tzinfo=UTC)) == (
        "2025-01-02T03:04:05.123Z"
    )


def test_timestamp_serialization_rejects_naive_values():
    from datetime import datetime

    with pytest.raises(ValueError, match="timezone-aware"):
        format_timestamp(datetime(2025, 1, 2, 3, 4, 5))
