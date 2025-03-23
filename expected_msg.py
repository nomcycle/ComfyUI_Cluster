from .synced_random import SyncedRandom

EXPECT_DISTRIBUTE_PROMPT_MSG_KEY: int = abs(SyncedRandom.get_random_int())
FANOUT_EXPECTED_MSG_KEY: int = abs(SyncedRandom.get_random_int())
FANIN_EXPECTED_MSG_KEY: int = abs(SyncedRandom.get_random_int())
GATHER_EXPECTED_MSG_KEY: int = abs(SyncedRandom.get_random_int())
BEGIN_BUFFER_EXPECTED_MSG_KEY: int = abs(SyncedRandom.get_random_int())
