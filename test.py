from promise import Promise
import time
p = Promise.resolve(10)
r = p.then(lambda v: v * 2).await_result(1)
assert r == 20
results = Promise.all([Promise.resolve(1), Promise.resolve(2), Promise.resolve(3)]).await_result(1)
assert results == [1, 2, 3]
try:
    Promise.reject(ValueError("err")).await_result(1)
    assert False
except ValueError: pass
print("Promise tests passed")