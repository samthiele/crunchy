import crunchy
import crunchy.workflows.dummy # replace this to import your custom workflow
from pathlib import Path

# run crunchy server example
if __name__ == "__main__":


    crunchy.debug = True # set as false to disable printouts

    # setup crunchy settings (defaults)
    crunchy.crunchy_settings['nthreads']['value'] = 5
    
    # which directory should crunchy run in?
    home = str(Path.home())

    # run!
    # (be sure to do this inside a __name__ == '__main__' block to avoid nasty threading bugs
    #  on windows machines (duplication of the Flask app) )
    from crunchy.app import run
    run(home)