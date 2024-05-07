"""

A dummy workflow that constructs several noisy images, applies a median filter to them, and then
uses a histogram equalisation method to scale them to the same range.

Unlike real applications, these functions have intentionally written to be slow to demonstrate threading
capabilities in a more realistic sense.

Sam Thiele; 2022

"""

import numpy as np
import os
import crunchy
from crunchy.base.trigger import fileTrigger, init, finish
import crunchy.base.trigger as trigger

"""
All workflows can/should provide a name variable, which will be used to select in through the app and for
logging purposes.
"""
name = "Dummy"


"""
All workflows can/should define a settings dictionary. This will contain a set of dictionaries that describe
the possible settings values and their default. This is used to populate the settings dict that is passed to 
worker functions, and allow values to be changed from their default via the crunchy app.

The following includes an example of each of the different types of allowed setting types.
"""
crunchy.workflow_settings = dict(
    xdim = dict(type='int', value=512, min=256, max=4096, desc='a bounded integer. Represented in app by a slider.'),
    ydim = dict(type='int', value=512, min=256, max=4096, desc='another bounded integer'),
    noise = dict(type='float', value=3, min=1, max=10, desc='a bounded float. Represented in app by a slider.'),
    mode = dict(type='select', value='RGB', options=['RGB','RGBA','Greyscale'],desc=' a categorical string (dropdown box in app)'),
    format = dict(type='select', value='jpg', options=['png','bmp','jpg'],desc=''),
    name = dict(type='string', value='bigdata',desc='free text (a text box in app).'),
    set_count = dict(type='int', value=8, min=3, max=8,desc=''),
    assemble = dict(type='bool', value=True,desc='boolean variables (represented by check boxes in app).'),
    workflow_stage = dict( type='int', value=0, min=0, max=3,desc='staging variable for multi-step workflows.' ),
)

"""
All workflows can/should define a JinJa template for the "monitor" page. This can be used to provide custom visualisation
of the datasets being processed by crunchy. 
"""
crunchy.dashboard = """

{% extends 'base.html' %}

{% block header %}
<meta http-equiv="refresh" content="10">
{% endblock %}

{% block content %}
<h1>Dummy monitor</h1>
<p>Display useful real-time monitoring data here. This page has access to the entire settings dictionary (crunchy.settings) 
   and functions such as <i>os</i>, <i>re</i>, <i>glob</i>, <i>natsort</i> and <i>listdir</i>. 
   For linking to files or images, the root path of crunchy is provided as the variable <i>root</i>.</p>
<hr>
{%for f in glob( os.path.join(outpath,"*/comp.image.png") )%}
    <h1>{{os.path.basename(os.path.dirname(f))}}</h1>
    <img src="{{os.path.relpath(f,root)}}" alt={{f}}>
    <p>Result file stored in: {{os.path.relpath(f,root)}}</p>
{%endfor%}
{% if len(glob( os.path.join(outpath,"*/comp.image.png"))) == 0 %}
    <p>No results have been generated yet</p>
{% endif %}
{% endblock %}'
"""


@init
def setup( indir, outdir, settings ):
    """

    This will be called after starting crunchy and before launching any worker threads. It is used here to setup
     hot directories (that are searched for files), and create some dummy input data.


    :param indir: a Path object to the input directory.
    :param outdir: a Path object to the output directory.
    :param settings: Workflow settings dictionary.
    :return: True if setup was successful (workflow should start), or False otherwise (workflow will be cancelled).
    """

    # create demo data in ouptut directory
    if 'set_count' not in settings:
        crunchy.log( "Error - invalid settings. Workflow cannot start.", crunchy.getLogDict() )
        return False

    set_count = settings['set_count']
    for j in range(3):
        o = '%s%d' % (settings['name'], j) # object name
        center = np.random.rand(2)
        for i in range(set_count):
            s = 'Set%d' % (i + 1)
            os.makedirs(indir / s / o, exist_ok=True)

            # create data files (containing random center pixels)
            np.save(indir / s / o / "point.npy", center + np.random.rand(2) * 0.01)

    # setup hot directories
    print("Building Input: ", indir)
    print("Output to: ", outdir)
    os.makedirs(outdir, exist_ok=True)
    crunchy.add(path = indir, depth=1, clear=False )
    crunchy.add(path = outdir, depth=0, clear=False )

    # progress to stage 1
    settings['workflow_stage'] = 1

    return True


"""
The following functions define the core of our workflow functionality.
"""
def build_image( data, outpath, settings ):
    """
    Workflow functions should all take three arguments, as described below.

    :param data: A dictionary with data stored as keys. Data can be added or retrieved from this dictionary
                 to facilitate information sharing within different workflow steps (running in the same thread).
    :param outpath: A directory where any results should be written.
    :param settings: A dictionary of settings that can be modified / changed by the GUI. Use for e.g. customisable
                     arguments.
    :return: Anything returned by workflow functions will be ignored. Results should be written to files instead
             (as other methods for passing values between threads is slow and expensive).
    """

    # parse settings
    xdim = settings["xdim"]
    ydim = settings["ydim"]
    if 'Greyscale' in settings["mode"]:
        bands = 1
    elif 'RGB' in settings["mode"]:
        bands = 3
    elif 'RGBA' in settings["mode"]:
        bands = 4

    # initialise the array
    arr = np.zeros( (xdim,ydim,bands) )
    a = 5 + 5*np.random.rand() # random signal amplitude

    # load center pixel
    c = np.load(data['input'])
    cx = int(c[0] * xdim) # center of signal
    cy = int(c[1] * ydim) # center of signal

    # fill it with random numbers and a nice wave pattern
    # N.B. this is done with loops so as to be as sloow as possible ;-)
    for x in range(xdim):
        for y in range(ydim):

            # populate with signal (sign wave)
            r = np.sqrt( (x-cx)**2 + (y-cy)**2 )
            arr[x,y,:] = np.sin( 32 * r / xdim) * a**(-np.clip(r / xdim, 0.5, 1.0 ) )

    # store in data dictionary to make accessible to other functions
    data['image'] = arr

def add_noise( data, outpath, settings ):
    sigma = settings['noise']
    arr = data['image']
    noise = np.zeros_like(arr)
    for x in range(arr.shape[0]):
        for y in range(arr.shape[1]):
            for z in range(arr.shape[2]):
                noise[x,y,z] = np.random.rand()*sigma
                arr[x,y,z] += noise[x,y,z]

    # write noisy image to output directory
    fname = "%s_%d.image.npy" % (data['name'], os.getpid())
    np.save( outpath / fname, arr )

def save_image( data, outpath, settings ):
    fmt = settings['format']
    name = data['name']
    arr = data['image']

    # normalise to range 0 - 255 and cast to uint8
    mn, mx = np.percentile(arr, (1, 99))
    arr = (np.clip((arr - mn) / (mx - mn), 0, 1) * 255).astype(np.uint8)

    if arr.shape[-1] == 1:
        arr = np.dstack([ arr[...,0], arr[...,0], arr[...,0] ] )

    # write image
    fname = "%s.image.%s" % (name, fmt)

    from PIL import Image # try importing PIL. This will fail if not installed
    image = Image.fromarray(arr)
    image.save(outpath / fname)

"""
And finally we define entry points that trigger the workflow execution. The simplest of these is a file filter,
which checks files or directories picked up by crunchy.scout. If yes, it should return the output path where
processed results should be put. Otherwise it should return None.

This function also has the chance to set up the data dictionary for the subsequent workflow and also update
the settings dictionary if required.

Note the function decorator that actually setups up the workflow sequence. This allows workflow functions to be
set up in different combinations or orders for different entry points.
"""

@fileTrigger(flow = [build_image, add_noise, save_image], fail=crunchy.base.errors.logAndStop, vb=3)
def process( data, outpath, settings ):
    #print(data['path'], os.path.exists(data['path']))
    path = data['path'] # this is the path being checked
    if os.path.exists(path / 'point.npy' ):

        # get file properties
        data['input'] = path / 'point.npy'
        data['name'] = path.parent.name

        # trigger processing and specify target directory for output
        return trigger.PROCESS, outpath / os.path.basename( path )

    # reject this folder
    return trigger.REJECT, ''

"""
We add a second file filter that averages the different images of each object together once they've been created 
to reduce noise. This demonstrates how file filters can also be used to perform assembly steps on results.
"""
def average( data, outpath, settings ):
    # load data
    assert 'files' in data, "Error - files must be specified."
    arr = np.array( [np.load(str(f))for f in data['files'] ] )
    settings['workflow_stage'] = 2  # upgrade workflow stage (just to test settings can be modified)
    # average it
    data['image'] = np.mean(arr, axis=0 )

@fileTrigger(flow = [average, save_image], fail=crunchy.base.errors.logAndStop, vb=3)
def assemble( data, outpath, settings ):
    files = list( data['path'].glob("*.image.npy"))
    comp = list( data['path'].glob("comp.image.png"))

    if settings['assemble']:
        if len(files) == settings['set_count']: # check all preprocessing has been done
            if len(comp) == 0: # check average hasn't already been created
                data['files'] = files # store list of files to average
                data['name'] ='comp' # output file name
                settings['format'] = 'png' # force output format to be png
                return trigger.PROCESS, data['path'] # output directory in same spot as input
    return trigger.REJECT, ''

@finish
def final_tasks( indir, outdir, settings ):
    """
    This will be run when all jobs are completed.
    """
    print("A job well done!")
