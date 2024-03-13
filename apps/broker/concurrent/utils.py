import threading

#
# class RWLock:
#     def __init__(self):
#         self.cond = threading.Condition()
#         self.reads = 0
#         self.writes = 0
#
#     def w_lock(self):
#         with self.cond.acquire():
#             while self.reads > 0:
#                 self.cond.wait()
#
#     def w_release(self):
#         pass
#
#     def of(self, l_type: LockType):
#         return self._r_lock if l_type is LockType.READ else self._r_lock
