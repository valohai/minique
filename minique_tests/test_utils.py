def test_cached_property_import(recwarn):
    """
    Test that importing cached_property from minique.utils works,
    but raises a DeprecationWarning.
    """
    from functools import cached_property as stdlib_cached_property

    from minique.utils import cached_property

    assert cached_property is stdlib_cached_property
    depwarn = next(w for w in recwarn if issubclass(w.category, DeprecationWarning))
    assert "minique.utils.cached_property is deprecated" in str(depwarn.message)
