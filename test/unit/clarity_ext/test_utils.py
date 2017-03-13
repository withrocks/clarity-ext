import unittest
from mock import Mock
from clarity_ext.utils import lazyprop


class UsesLazyProp:
    """This class is only used in the tests"""

    def __init__(self, fn):
        self.fn = fn

    @lazyprop
    def lazy_prop(self):
        return self.fn()


class TestUtils(unittest.TestCase):

    def test_lazy_prop_called_once(self):
        """
        Ensures that the lazy property is called only once, but returns the same value.
        """
        mock = Mock(return_value=100)
        prop = UsesLazyProp(mock)
        val1 = prop.lazy_prop
        val2 = prop.lazy_prop
        self.assertEqual(val1, 100)
        self.assertEqual(val1, val2)
        mock.assert_called_once()
