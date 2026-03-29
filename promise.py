#!/usr/bin/env python3
"""Promise pattern with then/catch chaining."""
import threading

class Promise:
    def __init__(self, executor=None):
        self._value = None; self._error = None
        self._state = "pending"
        self._then_cbs = []; self._catch_cbs = []
        self._lock = threading.Lock()
        self._event = threading.Event()
        if executor:
            try: executor(self._resolve, self._reject)
            except Exception as e: self._reject(e)

    def _resolve(self, value):
        with self._lock:
            if self._state != "pending": return
            self._value = value; self._state = "fulfilled"
            for cb in self._then_cbs: cb(value)
        self._event.set()

    def _reject(self, error):
        with self._lock:
            if self._state != "pending": return
            self._error = error; self._state = "rejected"
            for cb in self._catch_cbs: cb(error)
        self._event.set()

    def then(self, on_fulfilled):
        p = Promise()
        def handler(val):
            try: p._resolve(on_fulfilled(val))
            except Exception as e: p._reject(e)
        with self._lock:
            if self._state == "fulfilled": handler(self._value)
            elif self._state == "pending": self._then_cbs.append(handler)
        return p

    def catch(self, on_rejected):
        p = Promise()
        def handler(err):
            try: p._resolve(on_rejected(err))
            except Exception as e: p._reject(e)
        with self._lock:
            if self._state == "rejected": handler(self._error)
            elif self._state == "pending": self._catch_cbs.append(handler)
        return p

    def wait(self, timeout=None):
        self._event.wait(timeout)
        if self._state == "rejected": raise self._error
        return self._value

    @staticmethod
    def resolve(value):
        return Promise(lambda res, rej: res(value))

    @staticmethod
    def reject(error):
        return Promise(lambda res, rej: rej(error))

    @staticmethod
    def all(promises):
        results = [None] * len(promises)
        remaining = [len(promises)]
        p = Promise()
        lock = threading.Lock()
        for i, pr in enumerate(promises):
            def make_handler(idx):
                def handler(val):
                    with lock:
                        results[idx] = val
                        remaining[0] -= 1
                        if remaining[0] == 0: p._resolve(results)
                return handler
            pr.then(make_handler(i))
            pr.catch(lambda e: p._reject(e))
        return p

if __name__ == "__main__":
    p = Promise.resolve(42).then(lambda x: x * 2)
    print(p.wait())

def test():
    # Resolve
    p = Promise.resolve(42)
    assert p.wait() == 42
    # Then chain
    p2 = Promise.resolve(10).then(lambda x: x * 2).then(lambda x: x + 1)
    assert p2.wait(timeout=1) == 21
    # Reject + catch
    p3 = Promise.reject(ValueError("oops")).catch(lambda e: f"caught: {e}")
    assert p3.wait(timeout=1) == "caught: oops"
    # All
    pa = Promise.all([Promise.resolve(1), Promise.resolve(2), Promise.resolve(3)])
    assert pa.wait(timeout=1) == [1, 2, 3]
    # Executor
    p4 = Promise(lambda res, rej: res("async"))
    assert p4.wait() == "async"
    print("  promise: ALL TESTS PASSED")
