from typing import Any

import obstore

import workstate
import workstate.obstore


def test_protocol():
    _: workstate.StateManager = workstate.obstore.StateManager[Any](
        obstore.store.MemoryStore()
    )
