from ..source import get_source_collection, open_and_sort

#
#
# @pytest.mark.parametrize("x, y", [(1, 2), (2, 3), (3, 4)])
# def test_add_one(x, y):
#    assert y == add_one(x)


def test_source_collection():
    get_source_collection(["tas", "pr"], frequency="mon", driving_source_id="ERA5")
