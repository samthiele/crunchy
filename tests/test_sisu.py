import unittest
import os
from tempfile import mkdtemp
from pathlib import Path
import time
import shutil

# import crunchy
import crunchy

# fill path to test dataset!
dotest = True
testdata =  r"/Users/thiele67/Documents/data/test_data/SiSuRockTest2"
#refdata = r"/Users/thiele67/Documents/data/test_data/SiSuRockTest/manual_refdata/crunchycoreg.npz"
refdata = 'REFDATA'
outpath = r"/Users/thiele67/Documents/data/test_data/SiSuRockTest2/_output" # keep the outputs for inspection
# outpath = None # use a temp directory that is automatically deleted

# import the workflow we will use
if dotest:
    import crunchy.workflows.sisurock

class MyTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # setup output path directory
        if outpath is None:
            cls.base_path = Path(mkdtemp())
        else:
            cls.base_path = Path(outpath)

        # apply settings
        crunchy.crunchy_settings['inpath']['value'] = testdata
        crunchy.crunchy_settings['outpath']['value'] = cls.base_path
        crunchy.crunchy_settings['nthreads']['value'] = 3
        crunchy.workflow_settings['calibration']['value'] = refdata
        crunchy.workflow_settings['project']['value'] = 'CrunchyTest'

        # load file list
        cls.test_datasets = []
        for f in os.listdir(testdata):
            s = os.path.join(testdata, f)
            if os.path.isdir(s):
                for f2 in os.listdir(s):
                    cls.test_datasets.append(os.path.join(s, f2))
        assert len(cls.test_datasets), "Error - no valid test data found at %s" % testdata



        # populate self.settings for isolated testing
        cls.settings = {}
        for k, v in crunchy.workflow_settings.items():
            cls.settings[k] = v['value']
        for k, v in crunchy.crunchy_settings.items():
            cls.settings[k] = v['value']

    @classmethod
    def tearDownClass(cls):
        # remove output path directory if in a temp location
        if outpath is None:
            print("Cleaning up after tests.")
            shutil.rmtree(cls.base_path)  # delete temp directory

    def test_get_dict(self):
        """
        Test workflow functions in isolation to check they all run.
        :return:
        """

        if dotest:
            # test scrape function
            from crunchy.workflows.sisurock import getMetaDict

            nmeta = 0
            for p in self.test_datasets:
                meta = getMetaDict( p, True )
                if isinstance(meta, dict):
                    nmeta+=1
                    self.assertTrue('sensor type' in meta) # check valid key in metadata
            self.assertGreater(nmeta, 0) # at least one folder should have metadata succesfully extracted

    def test_SiSu(self):
        if dotest:
            import crunchy.workflows.sisurock
            #self.assertTrue('calibrate' in crunchy.entries)  # check entry has been registered

            # setup crunchy (shared memory etc)
            crunchy.init(6) # four worker threads
            crunchy.settings['wait'] = 1  # how many times to visit a file before passing it to the workflow (to check stable size)
            crunchy.settings['idle'] = 0.1  # how long to sleep between checks

            # set output directory (this will be created if needed)
            crunchy.setInpath(testdata)
            crunchy.setOutpath(self.base_path)

            # run crunchy workflow
            crunchy.run()

            # wait until calibration is done
            n = 0
            while crunchy.settings['workflow_stage'] == 0 and n < 10:
                time.sleep(15.) # sleep for a bit while calibration is done and job begins
                n + 1
            self.assertTrue(n < 10, msg="Error - calibration stage never ended.")

            # give main workflow a chance to start
            time.sleep(15.) # ensure some time for actual workflow jobs to start

            # wait for jobs to finish
            crunchy.complete()

            crunchy.printLog()

            successCalib = False
            successJob = False
            for k, v in crunchy.logdict.items():
                if 'saveRefData' in v:  # check that the job actually ran!
                    successCalib = True
                if 'saveTray' in v:
                    successJob = True
            self.assertTrue(successJob or successCalib, msg='Error - calibration or processing job failed')

if __name__ == '__main__':
    if not os.path.exists(testdata):
        unittest.main()
    else:
        print("Error: test dataset at %s does not exist. Please update testdata variable. NO TESTS RAN!" % testdata)


