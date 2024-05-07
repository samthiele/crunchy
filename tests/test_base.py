import unittest
import os
from tempfile import mkdtemp
from pathlib import Path
import numpy as np
import time
import shutil
import crunchy

class MyTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """
            Construct a directory containing dummy data for processing
            :return: a file path to the directory
            """

        cls.base_path = Path( mkdtemp() )
        cls.input_path = 'DummyDataIn'
        cls.output_path = 'DummyDataOut'

        # build dummy data directory
        os.makedirs(cls.base_path / cls.input_path / "Sensor1", exist_ok=True)
        os.makedirs(cls.base_path / cls.input_path / "Sensor2", exist_ok=True)

        for o in ['Object1', 'Object2', 'Object3']:
            # create directory
            os.makedirs(cls.base_path /cls.input_path / "Sensor1" / o, exist_ok=True)
            os.makedirs(cls.base_path / cls.input_path / "Sensor2" / o, exist_ok=True)

            # create data files
            np.save(cls.base_path / cls.input_path / "Sensor1" / o / "data.npy", np.full(100, 10))  # sensor 1 data
            np.save(cls.base_path / cls.input_path / "Sensor2" / o / "data.npy", np.full(100, 20))  # sensor 2 data
    @classmethod
    def tearDownClass(cls):
        print("Cleaning up after tests.")
        shutil.rmtree(cls.base_path)  # delete temp directory

    def test_mirror_scrape(self):

        # test scrape function
        from crunchy.base.mirror import _scrape_
        dirs = _scrape_( str( self.base_path/self.input_path ), str(self.base_path/self.input_path) )
        self.assertTrue('Sensor1/Object1/data.npy' in dirs )
        self.assertTrue('Sensor1/Object2/data.npy' in dirs)

    def test_mirror(self):
        from crunchy.base.mirror import mirror

        indir = self.base_path / self.input_path  # files in here will be copied
        outdir = self.base_path / 'mirrorOut'  # into this directory

        # run mirror function
        mirror(indir, outdir, debug=True, maxiter=2, sleeptime=0.5)

        # add random additional file
        random = np.random.rand(10)
        np.save(indir / 'randomthing.npy', random )

        # mirror it
        mirror(indir, outdir, debug=True, maxiter=2, sleeptime=0.5)

        # check outputs have been copied
        self.assertTrue(os.path.exists(outdir / 'Sensor2/Object3/data.npy'))
        self.assertTrue(os.path.exists(outdir / 'randomthing.npy'))
        self.assertEqual( random[0], np.load(indir / 'randomthing.npy')[0]) # check the data was actually copied!

    def test_scout(self):
        from crunchy.base.scout import scout

        new_files = {}
        known_files = {}
        file_size_dict={}

        # scout( paths, depth, new_files, known_files, file_size_dict,  wait = 5, idle=5.0, maxiter=np.inf  )
        scout( [self.base_path], depth=2, new_files=new_files, known_files=known_files,
                                file_size_dict=file_size_dict,wait=5,idle=0.1,maxiter=2)

        print(file_size_dict)
        self.assertEqual( # file size
            file_size_dict[ str(self.base_path / 'DummyDataIn/Sensor1/Object1')][0],928)
        self.assertEqual( # visited twice with no change to file size
            file_size_dict[ str(self.base_path / 'DummyDataIn/Sensor1/Object1')][1], (2))

        # change file size
        arr = np.load(self.base_path / 'DummyDataIn/Sensor1/Object1/data.npy')
        np.save( self.base_path / 'DummyDataIn/Sensor1/Object1/data.npy', arr.astype(np.uint8) )

        scout([self.base_path], depth=2, new_files=new_files, known_files=known_files,
              file_size_dict=file_size_dict, wait=5, idle=0.1, maxiter=4)

        # check that Object1 is still in file size dict
        self.assertEqual(  # file size
            file_size_dict[str(self.base_path / 'DummyDataIn/Sensor1/Object1')][0], 228)

        # check that the other objects have been moved to the new files list
        self.assertTrue( str(self.base_path / 'DummyDataIn/Sensor1/Object2') in new_files)

    def test_workers(self):
        """
        Setup crunchy queue then tear it down, and check that threads are initialising correctly etc.
        """
        # init crunchy
        nthreads=2
        crunchy.init(nthreads)

        time.sleep(1)

        # pause to check blocking works
        crunchy.pause()

        time.sleep(1)

        # check logs are set up
        self.assertEqual(len(crunchy.logdict), nthreads+1)
        for k,v in crunchy.logdict.items():
            self.assertFalse('complete' in v.lower()) # complete should not have run yet

        # unpause to run complete commands
        crunchy.resume()

        # shutdown
        crunchy.complete()

        # print logs
        print("------ INIT TEST OUTPUT ----- ")
        for k, v in crunchy.logdict.items():
            print("Log for process %s:" % k)
            print(v)
        print("------------------------------")
        self.assertEqual(len(crunchy.logdict), nthreads+1)
        for k,v in crunchy.logdict.items():
            if isinstance(k, int):
                self.assertTrue('complete' in v.lower())


if __name__ == '__main__':
    unittest.main()
