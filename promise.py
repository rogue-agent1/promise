#!/usr/bin/env python3
"""JavaScript-style Promises with then/catch/all/race."""
import sys, threading, time, traceback

class Promise:
    PENDING, FULFILLED, REJECTED = 0, 1, 2

    def __init__(self, executor=None):
        self._state = self.PENDING; self._value = None; self._callbacks = []
        self._lock = threading.Lock(); self._event = threading.Event()
        if executor:
            def resolve(v): self._settle(self.FULFILLED, v)
            def reject(e): self._settle(self.REJECTED, e)
            try: executor(resolve, reject)
            except Exception as e: reject(e)

    def _settle(self, state, value):
        with self._lock:
            if self._state != self.PENDING: return
            self._state = state; self._value = value; self._event.set()
        for cb in self._callbacks: cb(state, value)

    def then(self, on_fulfilled=None, on_rejected=None):
        def executor(resolve, reject):
            def callback(state, value):
                try:
                    if state == Promise.FULFILLED:
                        result = on_fulfilled(value) if on_fulfilled else value
                    else:
                        if on_rejected: result = on_rejected(value)
                        else: reject(value); return
                    if isinstance(result, Promise): result.then(resolve, reject)
                    else: resolve(result)
                except Exception as e: reject(e)
            with self._lock:
                if self._state == self.PENDING: self._callbacks.append(callback)
                else: callback(self._state, self._value)
        return Promise(executor)

    def catch(self, on_rejected): return self.then(None, on_rejected)

    def wait(self, timeout=None):
        self._event.wait(timeout)
        if self._state == self.REJECTED: raise self._value if isinstance(self._value, Exception) else Exception(self._value)
        return self._value

    @staticmethod
    def resolve(value):
        p = Promise(); p._settle(Promise.FULFILLED, value); return p

    @staticmethod
    def reject(reason):
        p = Promise(); p._settle(Promise.REJECTED, reason); return p

    @staticmethod
    def all(promises):
        results = [None] * len(promises); count = [0]
        def executor(resolve, reject):
            if not promises: resolve([]); return
            for i, p in enumerate(promises):
                def on_ok(v, idx=i):
                    results[idx] = v; count[0] += 1
                    if count[0] == len(promises): resolve(results)
                p.then(on_ok, reject)
        return Promise(executor)

    @staticmethod
    def race(promises):
        def executor(resolve, reject):
            for p in promises: p.then(resolve, reject)
        return Promise(executor)

def async_task(value, delay=0.1):
    def executor(resolve, reject):
        def run():
            time.sleep(delay)
            if isinstance(value, Exception): reject(value)
            else: resolve(value)
        threading.Thread(target=run, daemon=True).start()
    return Promise(executor)

def main():
    print("=== Promise Library Demo ===\n")
    print("Basic chain:")
    result = async_task(5, 0.05).then(lambda x: x * 2).then(lambda x: x + 1).wait(timeout=2)
    print(f"  5 -> *2 -> +1 = {result}")

    print("\nError handling:")
    try:
        async_task(Exception("oops"), 0.05).catch(lambda e: f"caught: {e}").wait(timeout=2)
    except: pass
    recovered = async_task(Exception("fail"), 0.05).catch(lambda e: "recovered").wait(timeout=2)
    print(f"  Recovered: {recovered}")

    print("\nPromise.all:")
    promises = [async_task(i, 0.02 * i) for i in range(5)]
    results = Promise.all(promises).wait(timeout=2)
    print(f"  All: {results}")

    print("\nPromise.race:")
    fast = async_task("fast", 0.01); slow = async_task("slow", 0.1)
    winner = Promise.race([fast, slow]).wait(timeout=2)
    print(f"  Winner: {winner}")

if __name__ == "__main__": main()
