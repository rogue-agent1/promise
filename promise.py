#!/usr/bin/env python3
"""promise - Promise/Future implementation with chaining and combinators."""
import sys, threading, time

class Promise:
    PENDING = "pending"
    FULFILLED = "fulfilled"
    REJECTED = "rejected"
    
    def __init__(self, executor=None):
        self.state = self.PENDING
        self.value = None
        self.error = None
        self.callbacks = []
        self.errbacks = []
        self.lock = threading.Lock()
        
        if executor:
            try:
                executor(self._resolve, self._reject)
            except Exception as e:
                self._reject(e)
    
    def _resolve(self, value):
        with self.lock:
            if self.state != self.PENDING:
                return
            self.state = self.FULFILLED
            self.value = value
            for cb in self.callbacks:
                cb(value)
    
    def _reject(self, error):
        with self.lock:
            if self.state != self.PENDING:
                return
            self.state = self.REJECTED
            self.error = error
            for eb in self.errbacks:
                eb(error)
    
    def then(self, on_fulfilled=None, on_rejected=None):
        result = Promise()
        
        def handle_fulfill(value):
            try:
                if on_fulfilled:
                    v = on_fulfilled(value)
                    if isinstance(v, Promise):
                        v.then(result._resolve, result._reject)
                    else:
                        result._resolve(v)
                else:
                    result._resolve(value)
            except Exception as e:
                result._reject(e)
        
        def handle_reject(error):
            try:
                if on_rejected:
                    v = on_rejected(error)
                    result._resolve(v)
                else:
                    result._reject(error)
            except Exception as e:
                result._reject(e)
        
        with self.lock:
            if self.state == self.FULFILLED:
                handle_fulfill(self.value)
            elif self.state == self.REJECTED:
                handle_reject(self.error)
            else:
                self.callbacks.append(handle_fulfill)
                self.errbacks.append(handle_reject)
        
        return result
    
    def catch(self, on_rejected):
        return self.then(None, on_rejected)
    
    def wait(self, timeout=10):
        event = threading.Event()
        def done(*_): event.set()
        self.then(done, done)
        if self.state != self.PENDING:
            return self
        event.wait(timeout)
        return self
    
    @staticmethod
    def resolve(value):
        return Promise(lambda res, _: res(value))
    
    @staticmethod
    def reject(error):
        return Promise(lambda _, rej: rej(error))
    
    @staticmethod
    def all(promises):
        result = Promise()
        values = [None] * len(promises)
        remaining = [len(promises)]
        
        if not promises:
            result._resolve([])
            return result
        
        for i, p in enumerate(promises):
            def make_handler(idx):
                def handler(v):
                    values[idx] = v
                    remaining[0] -= 1
                    if remaining[0] == 0:
                        result._resolve(values)
                return handler
            p.then(make_handler(i), result._reject)
        return result
    
    @staticmethod
    def race(promises):
        result = Promise()
        for p in promises:
            p.then(result._resolve, result._reject)
        return result

def test():
    # Basic resolve
    p = Promise.resolve(42)
    assert p.state == "fulfilled"
    assert p.value == 42
    
    # Chaining
    result = []
    p.then(lambda v: v * 2).then(lambda v: result.append(v))
    assert result == [84]
    
    # Reject + catch
    p2 = Promise.reject(ValueError("boom"))
    caught = []
    p2.catch(lambda e: caught.append(str(e)))
    assert caught == ["boom"]
    
    # Async
    p3 = Promise(lambda res, rej: threading.Timer(0.05, lambda: res("async")).start())
    p3.wait(1)
    assert p3.value == "async"
    
    # All
    pa = Promise.all([Promise.resolve(1), Promise.resolve(2), Promise.resolve(3)])
    pa.wait()
    assert pa.value == [1, 2, 3]
    
    # Race
    slow = Promise(lambda res, _: threading.Timer(1, lambda: res("slow")).start())
    fast = Promise.resolve("fast")
    pr = Promise.race([slow, fast])
    pr.wait()
    assert pr.value == "fast"
    
    # Error propagation
    errors = []
    Promise.resolve(1).then(lambda v: 1/0).catch(lambda e: errors.append(type(e).__name__))
    assert errors == ["ZeroDivisionError"]
    
    print("All tests passed!")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test()
    else:
        print("Usage: promise.py test")
