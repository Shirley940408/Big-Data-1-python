import random

from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# add more functions as necessary
def expected_sum():
    s = 0.0
    iteration = 0
    while s < 1:
        s += random.random()
        iteration += 1
    return iteration

def main(inputs):
    # main logic starts here
    samples = pow(2, int(inputs))
    ranges = sc.range(samples,numSlices=128)
    times = ranges.map(expected_sum)
    total_iters = times.reduce(lambda x, y: x + y)
    print(total_iters/times)

if __name__ == '__main__':
    conf = SparkConf().setAppName('euler')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    sample_power = sys.argv[1]
    main(sample_power)