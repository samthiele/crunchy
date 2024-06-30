import crunchy
from crunchy.app import run
import os

# todo; add an import to your workflow file here

if __name__ == '__main__':
    print("Launching: ", os.getpid())

    # setup crunchy
    crunchy.crunchy_settings['nthreads']['value'] = 3
    crunchy.crunchy_settings['inpath']['value'] = 'CrunchyIn'
    crunchy.crunchy_settings['outpath']['value'] = 'CrunchyOut'
    crunchy.crunchy_settings['wait']['value'] = 1

    crunchy.workflow_settings['project']['value'] = 'Test'

    # run gui
    run('/Users/thiele67/Documents/data/test_data')
