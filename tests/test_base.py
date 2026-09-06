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
        keys = {Path(k) for k in dirs}
        self.assertIn(Path('Sensor1') / 'Object1' / 'data.npy', keys)
        self.assertIn(Path('Sensor1') / 'Object2' / 'data.npy', keys)

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
        # Other test modules (e.g. test_dummy) register @finish handlers on import.
        # Isolate this test so complete() does not log from those handlers.
        saved_finalize = dict(crunchy.finalize)
        crunchy.finalize.clear()
        try:
            # init crunchy
            nthreads=2
            crunchy.init(nthreads)

            time.sleep(1)

            # pause to check blocking works
            crunchy.pause()

            time.sleep(1)

            # check logs are set up (flush worker lines first)
            logs = crunchy.getLogDict()
            self.assertEqual(len(logs), nthreads+1)
            for k,v in logs.items():
                self.assertFalse('complete' in v.lower()) # complete should not have run yet

            # unpause to run complete commands
            crunchy.resume()

            # shutdown
            crunchy.complete()

            # print logs
            logs = crunchy.getLogDict()
            print("------ INIT TEST OUTPUT ----- ")
            for k, v in logs.items():
                print("Log for process %s:" % k)
                print(v)
            print("------------------------------")
            self.assertEqual(len(logs), nthreads+1)
            for k,v in logs.items():
                if isinstance(k, int):
                    self.assertTrue('complete' in v.lower())
        finally:
            crunchy.finalize.update(saved_finalize)

    def test_config_file(self):
        """INI / JSON defaults apply to crunchy_settings and matching workflow keys."""
        import crunchy.workflows.dummy  # registers workflow_settings
        saved_c = {k: dict(v) for k, v in crunchy.crunchy_settings.items()}
        saved_w = {k: dict(v) for k, v in crunchy.workflow_settings.items()}
        ini = self.base_path / 'crunchy.ini'
        ini.write_text(
            "[crunchy]\nwait = 7\nram_reserve = 8.5\n"
            "[workflow]\nset_count = 3\nassemble = false\n",
            encoding='utf-8',
        )
        js = self.base_path / 'crunchy.json'
        js.write_text('{"crunchy": {"idle": 2.5}, "workflow": {"name": "fromjson"}}',
                      encoding='utf-8')
        sidecar = self.base_path / 'workflow.ini'
        sidecar.write_text("[workflow]\nproject = Field\nVNIR = false\n", encoding='utf-8')
        try:
            crunchy.apply_config(ini, force=True)
            self.assertEqual(crunchy.crunchy_settings['wait']['value'], 7)
            self.assertEqual(crunchy.crunchy_settings['ram_reserve']['value'], 8.5)
            self.assertEqual(crunchy.workflow_settings['set_count']['value'], 3)
            self.assertFalse(crunchy.workflow_settings['assemble']['value'])
            crunchy.apply_config(js, force=True)
            self.assertEqual(crunchy.crunchy_settings['idle']['value'], 2.5)
            self.assertEqual(crunchy.workflow_settings['name']['value'], 'fromjson')

            schema = dict(
                project=dict(type='string', value='Crunchy'),
                VNIR=dict(type='bool', value=True),
            )
            cfg = crunchy.read_ini(sidecar, schema)
            self.assertEqual(cfg['project'], 'Field')
            self.assertEqual(schema['project']['value'], 'Field')
            self.assertFalse(schema['VNIR']['value'])
            self.assertEqual(crunchy.read_ini(self.base_path / 'missing.ini', missing_ok=True), {})
        finally:
            for k, v in saved_c.items():
                crunchy.crunchy_settings[k].update(v)
            crunchy.workflow_settings.clear()
            for k, v in saved_w.items():
                crunchy.workflow_settings[k] = dict(v)

    def test_pref_cookie_roundtrip(self):
        """Launchpad cookie helpers write and restore setting values per workflow."""
        from crunchy.app import _prefs_payload, _apply_prefs, _workflow_pref_id
        saved_c = {k: dict(v) for k, v in crunchy.crunchy_settings.items()}
        saved_w = {k: dict(v) for k, v in crunchy.workflow_settings.items()}
        try:
            crunchy.crunchy_settings['inpath']['value'] = Path('/tmp/crunchy_in')
            crunchy.crunchy_settings['wait']['value'] = 9
            data = _prefs_payload()
            slot = data['by_workflow'][_workflow_pref_id()]
            self.assertEqual(slot['crunchy']['inpath'], '/tmp/crunchy_in')
            self.assertEqual(slot['crunchy']['wait'], 9)
            crunchy.crunchy_settings['inpath']['value'] = Path('CrunchyIn')
            crunchy.crunchy_settings['wait']['value'] = 5
            _apply_prefs(data)
            self.assertEqual(Path(crunchy.crunchy_settings['inpath']['value']), Path('/tmp/crunchy_in'))
            self.assertEqual(crunchy.crunchy_settings['wait']['value'], 9)

            # a different workflow's cookie must not overwrite this one's paths
            crunchy.workflow_settings.clear()
            crunchy.workflow_settings['project'] = dict(type='string', value='Field')
            crunchy.crunchy_settings['inpath']['value'] = Path('/sensorA')
            field_cookie = _prefs_payload()
            crunchy.workflow_settings.clear()
            crunchy.workflow_settings['xdim'] = dict(type='int', value=8)
            crunchy.crunchy_settings['inpath']['value'] = Path('/dummy_in')
            _apply_prefs(field_cookie)
            self.assertEqual(Path(crunchy.crunchy_settings['inpath']['value']), Path('/dummy_in'))
            crunchy.workflow_settings.clear()
            crunchy.workflow_settings['project'] = dict(type='string', value='Other')
            _apply_prefs(field_cookie)
            self.assertEqual(Path(crunchy.crunchy_settings['inpath']['value']), Path('/sensorA'))
            self.assertEqual(crunchy.workflow_settings['project']['value'], 'Field')
        finally:
            for k, v in saved_c.items():
                crunchy.crunchy_settings[k].update(v)
            crunchy.workflow_settings.clear()
            for k, v in saved_w.items():
                crunchy.workflow_settings[k] = dict(v)

    def test_daily_logs_and_disk(self):
        """Dated logs/YYYY-MM-DD files get errors and thread lines; disk probe works."""
        saved_finalize = dict(crunchy.finalize)
        crunchy.finalize.clear()
        logdir = self.base_path / 'dailylogs'
        try:
            crunchy.init(1)
            crunchy.settings['log_dir'] = str(logdir)
            crunchy.log('hello daily', crunchy.getLogDict(), True)
            crunchy.log_error('boom test', RuntimeError('xyz'))
            day = logdir / crunchy._log_day()
            session = day / 'log.txt'
            errors = day / 'errors.txt'
            self.assertTrue(session.is_file())
            self.assertIn('hello daily', session.read_text())
            self.assertTrue(errors.is_file())
            text = errors.read_text()
            self.assertIn('xyz', text)
            self.assertRegex(text, r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}')
            free, total = crunchy._disk_bytes(self.base_path)
            self.assertTrue(free is not None and total > 0)
            crunchy.complete()
        finally:
            crunchy.finalize.update(saved_finalize)

    def test_watch(self):
        extra = self.base_path / 'extra_hot'
        extra.mkdir()
        crunchy.watch(extra, depth=2, restart=False)
        self.assertEqual(crunchy.scoutdirs[str(extra)], 2)
        crunchy.scoutdirs.pop(str(extra), None)

    def test_file_overview_recheck(self):
        """Ignored paths show up in file_overview and recheck puts them on new_files."""
        saved_finalize = dict(crunchy.finalize)
        crunchy.finalize.clear()
        ignored = str(self.base_path / self.input_path / 'Sensor1' / 'Object1')
        try:
            crunchy.init(1)
            crunchy.known_files[ignored] = 100
            crunchy.file_filter[ignored] = [('process', 0)]
            snap = crunchy.file_overview()
            by_path = {r['path']: r for r in snap['rows']}
            self.assertIn(ignored, by_path)
            self.assertEqual(by_path[ignored]['state'], 'ignored')
            self.assertTrue(by_path[ignored]['can_recheck'])
            self.assertEqual(snap['counts'].get('ignored'), 1)
            n = crunchy.recheck(ignored)
            self.assertEqual(n, 1)
            self.assertNotIn(ignored, crunchy.known_files)
            self.assertIn(ignored, crunchy.new_files)
            self.assertNotIn(ignored, crunchy.file_filter)
            crunchy.complete()
        finally:
            crunchy.finalize.update(saved_finalize)

    def test_ram_guard(self):
        """Memory probe works on this OS; reserve 0 always allows work."""
        avail, total = crunchy._memory_bytes()
        self.assertTrue(
            (avail is None and total is None) or (avail >= 0 and total > 0)
        )
        self.assertTrue(crunchy._ram_ok({'ram_reserve': 0}))
        self.assertEqual(crunchy._cap_workers_for_ram(4, {'ram_reserve': 0}), 4)
        if avail is not None:
            self.assertFalse(crunchy._ram_ok({'ram_reserve': 1e6}))
            self.assertEqual(crunchy._cap_workers_for_ram(4, {'ram_reserve': 1e6}), 1)

    def test_reinit_after_complete(self):
        """A second init() after Finish must not talk to a dead Manager."""
        saved_finalize = dict(crunchy.finalize)
        crunchy.finalize.clear()
        try:
            crunchy.init(1)
            crunchy.complete()
            crunchy.init(1)
            self.assertTrue(crunchy.initialised())
            crunchy.complete()
            self.assertFalse(crunchy.initialised())
        finally:
            crunchy.finalize.update(saved_finalize)

    def test_pause_complete(self):
        """Finish must return even if workers were left paused."""
        saved_finalize = dict(crunchy.finalize)
        crunchy.finalize.clear()
        try:
            crunchy.init(1)
            crunchy.pause()
            crunchy.complete()
            self.assertFalse(crunchy.initialised())
        finally:
            crunchy.finalize.update(saved_finalize)

    def test_priority_queue(self):
        """Higher priority jobs are taken from the queue first."""
        saved_finalize = dict(crunchy.finalize)
        crunchy.finalize.clear()
        record = self.base_path / 'priority_order.txt'
        if record.exists():
            record.unlink()
        try:
            crunchy.init(1)
            crunchy.pause()
            q = crunchy.getQueue()
            from crunchy.base.trigger import _job
            for label, pri in (('low', 0), ('high', 10), ('mid', 5)):
                data = dict(path=label, record=str(record), label=label)
                q.put(("EXEC", (_job, ([_record_name], _noop_fail, None, data, str(self.base_path), {}), {})),
                      priority=pri)
            crunchy.resume()
            crunchy.complete()
            self.assertTrue(record.exists())
            order = record.read_text().strip().splitlines()
            self.assertEqual(order, ['high', 'mid', 'low'])
        finally:
            crunchy.finalize.update(saved_finalize)

    def test_worker_survives_error(self):
        """A job exception must not retire the worker; later jobs still run and are filed."""
        saved_finalize = dict(crunchy.finalize)
        crunchy.finalize.clear()
        record = self.base_path / 'after_error.txt'
        errfile = self.base_path / 'errorlog.txt'
        try:
            crunchy.init(1)
            crunchy.settings['error_log'] = str(errfile)
            crunchy.pause()
            q = crunchy.getQueue()
            from crunchy.base.trigger import _job
            from crunchy.base.errors import logAndStop
            q.put(("EXEC", (_job, ([_boom], logAndStop, None,
                                   dict(path='boom'), str(self.base_path), {}), {})),
                  priority=1)
            q.put(("EXEC", (_job, ([_ok], _noop_fail, None,
                                   dict(path='ok', record=str(record)), str(self.base_path), {}), {})),
                  priority=0)
            crunchy.resume()
            crunchy.complete()
            self.assertTrue(record.exists())
            self.assertEqual(record.read_text(), 'ok')
            self.assertTrue(errfile.exists())
            logged = errfile.read_text()
            self.assertIn('intentional boom', logged)
            self.assertIn('RuntimeError', logged)
            self.assertIn('Traceback', logged)
        finally:
            crunchy.finalize.update(saved_finalize)

    def test_worker_respawn(self):
        """A killed worker is replaced so the pool keeps running."""
        saved_finalize = dict(crunchy.finalize)
        crunchy.finalize.clear()
        try:
            crunchy.init(1)
            time.sleep(0.3)
            self.assertEqual(len(crunchy.workers), 1)
            old = crunchy.workers[0]
            old_pid = old.pid
            old.terminate()
            old.join(timeout=5)
            deadline = time.time() + 5
            replaced = False
            while time.time() < deadline:
                live = [w for w in crunchy.workers if w is not None and w.is_alive()]
                if any(w.pid != old_pid for w in live):
                    replaced = True
                    break
                time.sleep(0.2)
            self.assertTrue(replaced)
            crunchy.complete()
        finally:
            crunchy.finalize.update(saved_finalize)


def _record_name(data, outpath, settings):
    with open(data['record'], 'a') as fh:
        fh.write(data['label'] + '\n')


def _noop_fail(logdict, E, **kwargs):
    return False


def _boom(data, outpath, settings):
    raise RuntimeError('intentional boom')


def _ok(data, outpath, settings):
    with open(data['record'], 'w') as fh:
        fh.write('ok')


if __name__ == '__main__':
    unittest.main()
