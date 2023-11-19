import time
import datetime as dt
import random 

start = time.monotonic_ns()
data = [random.randint(0, 1000000) for _ in range(1000000)]
data.sort()
end = time.monotonic_ns()

print("Start: ", start)
print("End: ", end)
print("Time taken:", end-start)
