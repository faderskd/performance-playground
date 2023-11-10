import cProfile
from contextlib import contextmanager

import yappi


@contextmanager
def profile(filename='results', engine='yappi', clock='wall', output_type='pstat'):
    if engine == 'yappi':
        filename = f'{filename}.prof'
        yappi.set_clock_type(clock)
        try:
            yappi.start(builtins=True, profile_threads=True)
            yield
        finally:
            yappi.stop()
            stats = yappi.get_func_stats()
            stats.print_all()
            stats.save(filename, type=output_type)
    else:
        profile = cProfile.Profile()
        try:
            profile.enable()
            yield
        finally:
            profile.disable()
            profile.print_stats()
            profile.dump_stats(f'{filename}.prof')
