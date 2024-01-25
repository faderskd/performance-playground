import threading

from apps.broker.index.persistent_data import PagePointer


class LockManager:
    def __init__(self):
        self._internal_lock = threading.Lock()
        self._locks = dict()

    def get_lock(self, pointer: PagePointer) -> threading.Lock:
        with self._internal_lock:
            if pointer not in self._locks.keys():
                self._locks[pointer] = threading.Lock()  # make it RW lock
            return self._locks[pointer]

    def remove_lock(self, pointer: PagePointer):
        pass # TODO: remove lock when deleting node from  page_manager
