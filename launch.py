import crunchy
import crunchy.workflows.sisurock
from crunchy.app import run
from pathlib import Path

# run crunchy server example
if __name__ == "__main__":


    crunchy.debug = True # set as false to disable printouts

    # setup crunchy settings (defaults)
    crunchy.crunchy_settings['nthreads']['value'] = 5
    
    # run!
    home = str(Path.home()) # launch in home directory by default
    run(home)