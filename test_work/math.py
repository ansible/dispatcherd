from dispatcher.publish import task


# demo values of n in seconds
# 30 - 0.052
# 29 - 0.035
# 28 - 0.024
# 27 - 0.015
# 26 - 0.012
# 25 - 0.0097

@task(queue='test_channel')
def fibonacci(n):
    if n <= 1:
        return n
    else:
        return fibonacci(n-1) + fibonacci(n-2)
