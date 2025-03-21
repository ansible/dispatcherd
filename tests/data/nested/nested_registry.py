from dispatcherd.registry import DispatcherMethodRegistry

"""
This hosts a registry that has nothing in it
until the test test_surprise_registration looks up a method
in a not-yet-imported module, and then SURPRISE,
the module the method lives in registers the method.

But that will not happen until the test runs.
So do not tell the registry until then, meaning,
do not import methods.py before running that test.
"""

surprised_registry = DispatcherMethodRegistry()
