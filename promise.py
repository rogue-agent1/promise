#!/usr/bin/env python3
"""Promise/Future pattern. Zero dependencies."""
import threading, sys, time

class Promise:
    PENDING = "pending"
    FULFILLED = "fulfilled"
    REJECTED = "rejected"

    def __init__(self, executor=None):
        self._state = self.PENDING
        self._value = None
        self._error = None
        self._then_cbs = []
        self._catch_cbs = []
        self._lock = threading.Lock()
        self._event = threading.Event()
        if executor:
            try:
                executor(self._resolve, self._reject)
            except Exception as e:
                self._reject(e)

    def _resolve(self, value):
        with self._lock:
            if self._state != self.PENDING: return
            self._state = self.FULFILLED
            self._value = value
        self._event.set()
        for cb in self._then_cbs:
            cb(value)

    def _reject(self, error):
        with self._lock:
            if self._state != self.PENDING: return
            self._state = self.REJECTED
            self._error = error
        self._event.set()
        for cb in self._catch_cbs:
            cb(error)

    def then(self, on_fulfilled, on_rejected=None):
        p = Promise()
        def handle_fulfill(val):
            try:
                result = on_fulfilled(val)
                if isinstance(result, Promise):
                    result.then(p._resolve, p._reject)
                else:
                    p._resolve(result)
            except Exception as e:
                p._reject(e)
        with self._lock:
            if self._state == self.FULFILLED:
                handle_fulfill(self._value)
            elif self._state == self.REJECTED:
                if on_rejected: on_rejected(self._error)
                else: p._reject(self._error)
            else:
                self._then_cbs.append(handle_fulfill)
                if on_rejected: self._catch_cbs.append(on_rejected)
        return p

    def catch(self, on_rejected):
        return self.then(lambda v: v, on_rejected)

    def await_result(self, timeout=None):
        self._event.wait(timeout)
        if self._state == self.REJECTED:
            raise self._error
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
                        if remaining[0] == 0:
                            p._resolve(results)
                return handler
            pr.then(make_handler(i), p._reject)
        return p

if __name__ == "__main__":
    p = Promise(lambda resolve, reject: resolve(42))
    result = p.then(lambda v: v * 2).await_result(timeout=1)
    print(f"Result: {result}")
