from distutils.core import setup
from Cython.Build import cythonize
setup(
    name='execute_table py',
    ext_modules=cythonize('execute_table.py')
)

setup(
    name='execute_ap py',
    ext_modules=cythonize('execute_ap.py')
)

setup(
    name='execute_job py',
    ext_modules=cythonize('execute_job.py')
)

setup(
    name='rollback_table py',
    ext_modules=cythonize('rollback_table.py')
)

setup(
    name='compare_generate py',
    ext_modules=cythonize('compare_generate.py')
)

