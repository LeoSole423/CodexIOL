import pytest

from iol_review.profile import ProfileError, validate_profile


def test_profile_requires_core_fields():
    with pytest.raises(ProfileError, match="objectives"):
        validate_profile({"horizon": "5 years", "liquidity_needs": "none", "loss_tolerance": "low", "allowed_markets": ["argentina"], "allowed_instruments": ["acciones"]})


def test_profile_accepts_flexible_text():
    validate_profile({"objectives": "growth", "horizon": "long", "liquidity_needs": "none", "loss_tolerance": "moderate", "allowed_markets": ["argentina"], "allowed_instruments": ["acciones"], "notes": "free text"})
