"""Tests for :mod:`cryptobot_predictfun`.

A real package (not a bare directory) so pytest imports these modules as
``cryptobot_predictfun.tests.test_*`` — both venue test suites carry files with the same
basenames (test_ws_mirror, test_wallet_diagnostics, test_derive_creds,
test_backfill_settlement) and rootdir-relative import would collide.
"""
