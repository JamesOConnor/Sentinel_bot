import os
import unittest

import numpy as np

from Sentinel_bot import colour_balance_image
from settings import TEST_FIXTURE_PATH


class SentinelBotTests(unittest.TestCase):
    """
    Tests methods the bot uses
    """

    def setUp(self):
        self.raw_im = os.path.join(TEST_FIXTURE_PATH, 'raw_image.npy')
        self.processed_im = os.path.join(TEST_FIXTURE_PATH, 'colour_balanced_image.npy')

    def test_colour_balance_image(self):
        """
        Tests the colour balancing of raw imagery is working as expected
        """
        raw = np.load(self.raw_im)
        actual = colour_balance_image(raw)
        expected = np.load(self.processed_im)
        np.testing.assert_equal(np.array(actual), expected)