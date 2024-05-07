import unittest
import os
from tempfile import mkdtemp
from pathlib import Path
import time
import shutil

# import crunchy
import crunchy

# import the workflow we will use
import crunchy.workflows.dummy

# if not None, test files will be written here and not deleted.
# if None, they will be written to a temp directory and removed afterwards.
debugpath = None
#debugpath = '/Users/thiele67/Documents/data/tests/tests'

"""
Test dummy workflow functions.
"""
class MyTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """
            Construct a directory containing dummy data for processing
            :return: a file path to the directory
            """
        if debugpath is None:
            cls.base_path = Path( mkdtemp() )
        else:
            cls.base_path = Path(debugpath)

        cls.input_path = 'DummyDataIn'
        cls.output_path = 'DummyDataOut'
        crunchy.crunchy_settings['inpath']['value'] = cls.base_path / cls.input_path
        crunchy.crunchy_settings['outpath']['value'] = cls.base_path / cls.output_path
        set_count = crunchy.workflow_settings['set_count']['value'] # how many sets to create

        # also populate self.settings for isolated testing
        # populate settings
        cls.settings = {}
        for k, v in crunchy.workflow_settings.items():
            cls.settings[k] = v['value']
        for k, v in crunchy.crunchy_settings.items():
            cls.settings[k] = v['value']

    @classmethod
    def tearDownClass(cls):
        if debugpath is None:
            print("Cleaning up after tests.")
            shutil.rmtree(cls.base_path)  # delete temp directory

    def test_functions(self):
        """
        Test workflow functions in isolation to check they all run.
        :return:
        """
        # test scrape function
        import crunchy
        from crunchy.workflows.dummy import build_image, add_noise, save_image

        # data dictionary (starts with only input file defined
        data = dict( input = self.base_path / self.input_path / 'Set1/bigdata0/point.npy',
                     name = 'test' )
        outdir = self.base_path / self.output_path
        os.makedirs(outdir, exist_ok=True)

        # populate settings
        for k, v in crunchy.workflow_settings.items():
            crunchy.settings[k] = v['value']
        for k, v in crunchy.crunchy_settings.items():
            crunchy.settings[k] = v['value']

        # do tests
        from crunchy.workflows.dummy import setup
        setup( Path( self.settings['inpath'] ), Path( self.settings['outpath'] ), self.settings )

        print("Building signal... ", end='')
        build_image(data, outdir, crunchy.settings)
        self.assertTrue('image' in data)
        print("Adding noise...", end='')
        add_noise(data, outdir, crunchy.settings)
        print("Saving image...", end='')
        save_image(data, outdir, crunchy.settings)

    def test_file_trigger(self):
        import crunchy.workflows.dummy

        self.assertTrue('process' in crunchy.entries)  # check entry has been registered
        self.assertTrue('assemble' in crunchy.entries)  # check entry has been registered


        # setup crunchy (shared memory etc)
        crunchy.init(2)

        # build base dataset (simulate setup call)
        from crunchy.workflows.dummy import setup
        setup(Path(self.settings['inpath']), Path(self.settings['outpath']), self.settings)

        # populate settings
        for k, v in crunchy.workflow_settings.items():
            crunchy.settings[k] = v['value']
        for k, v in crunchy.crunchy_settings.items():
            crunchy.settings[k] = v['value']

        # import and runfile trigger
        from crunchy.workflows.dummy import process
        outpath = self.base_path / self.output_path
        path = self.base_path / self.input_path / ('Set1/%s1/'%crunchy.settings['name'])

        # pass to file trigger
        process(path, outpath, self.settings)

        time.sleep(2) # give time for job to be started

        # wait for job to finish
        crunchy.complete()
        crunchy.printLog()

        success = False
        for k,v in crunchy.logdict.items():
            #print("Thread %d:" % k)
            #print(v)
            if 'add_noise' in v: # check that the job actually ran!
                success=True
        self.assertTrue(success)

    def test_dummy(self):
        """
        Setup and run crunchy on complete dummy exercise.
        :return:
        """

        # setup crunchy and register workflow
        import crunchy.workflows.dummy  # this should set up and register workflow
        crunchy.init(7) # spawn seven worker threads

        # add a few settings
        crunchy.settings['wait'] = 1  # how many times to visit a file before passing it to the workflow (to check stable size)
        crunchy.settings['idle'] = 0.1  # how long to sleep between checks

        self.assertEqual(crunchy.settings['workflow_stage'], 0)

        # set output directory (this will be created if needed)
        crunchy.setOutpath( self.base_path / self.output_path )

        # initialise and run workflow (as scout thread finds files)
        crunchy.run()

        # check that stage was updated
        self.assertEqual(crunchy.settings['workflow_stage'], 1)

        # wait a bit to let magic processing happen
        time.sleep(60.)

        # wait until queue is empty and then end
        crunchy.complete()

        # check that stage was updated
        self.assertEqual(crunchy.settings['workflow_stage'], 2)

        # count outputs to check that everything ran smoothly!
        out = list( (self.base_path / self.output_path).glob("*/comp.image.png") )
        if len(out) < 3:
            crunchy.printLog()
        self.assertEqual(3,len(out))

if __name__ == '__main__':
    unittest.main()
