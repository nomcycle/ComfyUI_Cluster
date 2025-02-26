import random

class SyncedRandom:
    _SYNCED_SEED = 12456
    _initialized = False
    _synced_state = None

    @staticmethod
    def _ensure_initialized():
        if not SyncedRandom._initialized:
            random.seed(SyncedRandom._SYNCED_SEED)
            SyncedRandom._synced_state = random.getstate()
            SyncedRandom._initialized = True

    @staticmethod
    def get_random_int() -> int:
        current_state = random.getstate()
        SyncedRandom._ensure_initialized()

        random.setstate(SyncedRandom._synced_state)
        result = random.randint(-2147483648, 2147483647)
        SyncedRandom._synced_state = random.getstate()
        
        random.setstate(current_state)
        return result