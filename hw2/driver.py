import time
import sys

from dask.distributed import Client
from dask import delayed

import hw2
from hwfunctions import fun_factor, fun_inc

NUM_WORKERS = 4
THREADS_PER_WORKER = 2

def serial(fun, start, end):
    """ Computes the sum of fun applied to the numbers start, start+1, ..., end-1 

    fun is going to be either fun_inc or fun_factor
    """
    return sum([fun(x) for x in range(start, end)])


def tester(name, fun, par_fun, start, end, isdelayed):
    """ Compare your dask delayed parallel version to the serial version """
    with Client(n_workers=NUM_WORKERS, threads_per_worker=THREADS_PER_WORKER) as c:
        parallel_start = time.perf_counter()
        myref = par_fun(c, start, end)
        if isdelayed:
            answer_hw = myref.compute()
        else:
            answer_hw = c.gather(myref)
        parallel_end = time.perf_counter()
        if isdelayed:
            assert type(myref) == type(delayed(sum)([])), "Your function must return a Dask Delayed object"
        else:
            assert type(myref) == type(c.submit(sum, [])), "Your function must return a Dask Futures object"
        time_hw = parallel_end - parallel_start

    serial_start = time.perf_counter()
    answer = serial(fun, start, end)
    serial_end = time.perf_counter()
    time_serial = serial_end - serial_start

    print(name)
    if answer_hw != answer:
        print("Wrong Answer: {answer_hw}, expected: {answer}")
    print(f"Parallel time: {time_hw}")
    print(f"Serial time: {time_serial}")
    if time_hw > time_serial:
        print(f"Whoa, parallel time is slower!!!")
    print()


def main():
    # When you are testing, remember to try different start and end values
    # For example, in the first function call, start is 0 and end is 10000000
    # In python, you can add _ between digits for readability, so 10000000 is the
    # same as 10_000_000
    tester("Delayed for Increment", fun_inc, hw2.delayed_increment, 0, 10_000_000, True)
    tester("Delayed for Factor", fun_factor, hw2.delayed_factor, 3_000_000, 3_000_010, True)
    tester("Future for Increment", fun_inc, hw2.future_increment, 0, 10_000_000, False)
    tester("Future for Factor", fun_factor, hw2.future_factor, 3_000_000, 3_000_010, False)


if __name__ == "__main__":
    main()

