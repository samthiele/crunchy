from flask import Flask , render_template, render_template_string, send_file, abort, send_from_directory, request
from pathlib import Path
from natsort import natsorted
import crunchy
import re
from glob import glob

# set numpy to run in a single thread
import os
os.environ['OPENBLAS_NUM_THREADS'] = '1'
os.environ['MKL_NUM_THREADS'] = '1'

# globals
root = Path('/')
flaskapp = None
server = None

def run( basepath ):
    """
    Launch the crunchy app.
    """

    # store basepath for files and crunchy
    global root
    root = Path(basepath)

    # setup app
    global flaskapp
    flaskapp = Flask(__name__)
    flaskapp.config['TEMPLATES_AUTO_RELOAD'] = True

    @flaskapp.route('/', methods=['GET','POST'])
    def index():
        errors = []
        checked = [] # checkboxes that have been set - all others should be changed to False!
        launch = False # becomes true if we want (and can) launch crunchy!
        if request.method == 'POST': # handle POST requests
            for k,v in request.form.items():
                # print(k,v)
                try:
                    target, key = k.split('__')
                except:
                    print("Warning: could not parse input of name %s" % k)
                    continue

                if 'action' in target: # this was an action button
                    if 'launch' in key.lower():
                        launch = True
                    elif 'pause' in key.lower():
                        crunchy.pause()
                        crunchy.log('Crunchy is taking a break', crunchy.getLogDict(), True)
                    elif 'resume' in key.lower():
                        crunchy.resume()
                        crunchy.log('Resuming work', crunchy.getLogDict(), True)
                    elif 'finish' in key.lower():
                        crunchy.log('Finishing jobs... please wait', crunchy.getLogDict(), True)
                        crunchy.complete(True,True)
                        crunchy.log('Workflow complete', crunchy.getLogDict(), True)

                    elif 'terminate' in key.lower():
                        crunchy.complete(False, True)
                        crunchy.log('Workers have been brutally terminated.', crunchy.getLogDict(), True)
                else: # we are updating settings
                    if 'crunchy' in target: # update crunchy settings
                        target = crunchy.crunchy_settings
                    elif 'workflow' in target: # update  workflow settings
                        target = crunchy.workflow_settings

                    # convert to correct type (and validate that this is possible)
                    dtype = target[key].get('type','string').lower()
                    try:
                        if dtype == 'string' or dtype=='select':
                            v = str(v)
                        elif dtype == 'path':
                            v = Path(v)
                            if target[key].get('mustexist', False):
                                assert os.path.exists(root / v)
                        elif dtype == 'float':
                            v = float(v)
                        elif dtype == 'int':
                            v = int(v)
                        elif dtype == 'bool':
                            checked.append(key)
                            v = True # only "on" checkboxes are passed to the POST request
                    except:
                        errors.append(key)
                        continue
                    target[key]['value'] = v

            # If no errors, launch!
            if len(errors) == 0 and launch:
                # update paths to be absolute and booleans to be false unless set in the POST request
                for d in [crunchy.crunchy_settings, crunchy.workflow_settings]:
                    for k,v in d.items():
                        if v['type'] == 'path':
                            if str(root) not in str(v['value']):
                                v['value'] = root / v['value']
                        if v['type'] == 'bool':
                            if k not in checked:
                                v['value'] = False

                # spawn worker threads
                crunchy.init()
                crunchy.log('Starting workflow', crunchy.getLogDict(), True)
                thread = crunchy.run() # run workers
                if thread == None: # this failed
                    crunchy.complete(False, True)
                    launch = False

        # is crunchy running
        if crunchy.running(): # render status
            return render_template('index.html', log=crunchy.getLogDict(), crunchy=crunchy,
                                   reversed=reversed, len=len, enumerate=enumerate)
        else: # render settings / setup
            return render_template('settings.html', cset=crunchy.crunchy_settings, root=str(root),
                                   wset=crunchy.workflow_settings, errors=errors,
                                   enumerate=enumerate, )
    
    @flaskapp.route('/status', methods=['GET', 'POST'])
    def workflow():
        return render_template('status.html', settings=crunchy.settings, root=str(root), enumerate=enumerate, )
    
    @flaskapp.route('/debug', methods=['GET', 'POST'])
    def debug():
        crunchy.settings['outpath'] = '/Users/thiele67/Documents/data/test_data/CrunchyOut'
        crunchy.settings['inpath'] = '/Users/thiele67/Documents/data/test_data/CrunchyIn'
        crunchy.settings['workflow_stage'] = 1
        crunchy.settings['refdata'] = '/Users/thiele67/Documents/data/test_data/CrunchyOut/Digisort.hyc/REFDATA.hyc'
        crunchy.settings['drillholes'] = '/Users/thiele67/Documents/data/test_data/CrunchyOut/corestructure.hyc\n/Users/thiele67/Documents/data/test_data/CrunchyOut/SC21GWO030A.hyc'
        return render_template('dash.html', root=str(root),
                                      crunchy=crunchy, inpath=str(crunchy.settings['inpath']),
                                      outpath=str(crunchy.settings['outpath']),
                                      reversed=reversed, len=len, re=re, glob=glob, os=os, natsorted=natsorted, list=list)

    @flaskapp.route('/dashboard', methods=['GET', 'POST'])
    def dashboard():
        if request.method == 'POST': # handle POST requests
            pass
        else:
            if crunchy.running() and crunchy.initialised():

                return render_template_string( crunchy.dashboard, root=str(root),
                                               crunchy=crunchy, inpath=str(crunchy.settings['inpath']),
                                               outpath=str(crunchy.settings['outpath']),
                                               reversed=reversed, len=len, re=re, glob=glob, os=os, natsorted=natsorted)
            else:
                return "Crunchy is not running. Please start it before viewing dashboard."

    @flaskapp.route('/', defaults={'req_path': ''})
    @flaskapp.route('/<path:req_path>')
    def dir_listing(req_path):
        # Return 404 if path doesn't exist
        path = (root / req_path).resolve()
        if not os.path.exists(path):
            return abort(404)

        # Check if path is a file and serve
        if os.path.isfile(path):
            return send_file(path)
        else: # Show directory contents
            content = natsorted(os.listdir(path))
            files = [f for f in content if os.path.isfile(path / f)]
            dirs = [f for f in content if os.path.isdir(path / f)]
            if path != root:
                pdir = str(path.parent.relative_to(root))
            else:
                pdir = ''
            return render_template('files.html',
                                   parent_dir=pdir,
                                   dirs=dirs,
                                   files=files,
                                   basename=os.path.basename)

    # run
    try: # try first with default port
        flaskapp.run(threaded=False, processes=1, host="0.0.0.0", port=5001)
    except:
       flaskapp.run(threaded=False, processes=1, host="0.0.0.0", port=0) # find a random port

if __name__ == "__main__":
    import sys,os

    # parse arguments [ workflow basepath ]
    nworkers = os.cpu_count() - 1
    try:
        if len(sys.argv) == 1:
            print("Please specify workflow. Options are: dummy, sisurock")
        if len(sys.argv) >= 2:
            path = os.getcwd()
            flow = sys.argv[1] # workflow to run
        if len(sys.argv) >= 3:
            path = sys.argv[2] # path to run in
        if len(sys.argv) >= 4:
            nworkers = int(sys.argv[3]) # number of threads to use
    except:
        print("Error - incorrect arguments. Should be: workflow_name [base_path] [nthreads].")

    print("Launching crunchy workflow %s in %s" % (flow,path))
    import crunchy
    __import__("crunchy.workflows.%s"%flow) # import workflow
    print("Spawning %d worker threads."%nworkers)
    crunchy.init(nworkers)
    run( path )