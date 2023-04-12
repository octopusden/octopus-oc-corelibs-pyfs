# -*- coding: utf-8-*-
from __future__ import unicode_literals
from unittest import TestCase
import pysvn
from fs.errors import ResourceNotFound, FileExpected, DirectoryExpected, Unsupported
from fs.memoryfs import MemoryFS
from fs.mountfs import MountFS
import logging
from contextlib import contextmanager
from oc_pyfs.SvnFS import SvnFS, SvnReadonlyFS
from oc_pyfs.fs_utils import dir_info, file_info
from oc_pyfs.stubs.temp_svn_repo import TempRepo
from sys import version_info

if version_info.major == 3:
    import urllib.parse as urlparse;
else:
    import urlparse;


logging.basicConfig()


@contextmanager
def _suppress_expected_error(filter_fragment):
    class ExpectedLogSuppressor(object):

        def __init__(self):
            self.logged_records=[]
        
        def filter(self, record):
            self.logged_records.append(record)
            should_skip=(filter_fragment in record.msg)
            return 0 if should_skip else 1

    suppressor=ExpectedLogSuppressor()
    module_logger=logging.getLogger("oc_pyfs.SvnFS")
    module_logger.addFilter(suppressor)
    yield suppressor
    module_logger.removeFilter(suppressor)


def _in_test_repo(test_case):    
    def wrapped(suite, *args, **kwargs):
        with TempRepo() as repo:
            suite.repo=repo
            test_case(suite, *args, **kwargs)
    return wrapped
    

class FsApiTestSuite(TestCase):
    if version_info.major == 3:
        def assertItemsEqual(self, expected_seq, actual_seq, msg=None):
            return self.assertCountEqual( actual_seq, expected_seq, msg=msg );

    def _get_svn_fs(self):
        return SvnFS(self.repo.url, pysvn.Client())

    def _get_svn_fs_ro( self ):
        return SvnReadonlyFS( self.repo.url, pysvn.Client() );

    @_in_test_repo
    def test_failure_on_missing_url(self):
        with self.assertRaises(ResourceNotFound):
            SvnFS( urlparse.urljoin( self.repo.url,"foo/bar" ), pysvn.Client())

    @_in_test_repo
    def test_unknown_error_processed(self):
        class FailingClient(object):
            def info2(self, *args, **kwargs):
                err=pysvn.ClientError()
                err.args=("error message", (("error", 123), ("message", 456)))
                raise err
        with self.assertRaises(Unsupported), _suppress_expected_error("Unhandled") as log_suppressor:
            svn_fs=SvnFS(self.repo.url, FailingClient())
        log_records=log_suppressor.logged_records
        self.assertEqual(1, len(log_records))
        self.assertEqual(logging.ERROR, log_records[0].levelno)

    @_in_test_repo
    def test_listdir(self):
        self.repo.add_dir("a")
        self.repo.add_file("b")
        ls = self._get_svn_fs().listdir('/')
        self.assertItemsEqual(["a", "b"], ls)
        
    @_in_test_repo
    def test_listdir_expects_dir(self):
        self.repo.add_file("foo")
        with self.assertRaises(DirectoryExpected):
            self._get_svn_fs().listdir("foo")

    @_in_test_repo
    def test_listdir_fails_on_nonexistent_file(self):
        with self.assertRaises(ResourceNotFound):
            self._get_svn_fs().listdir("foo")

    @_in_test_repo
    def test_listdir_same_prefix_bug(self):
        self.repo.add_subtree("foo", ["foo_bar", "foo_baz", "bar", "baz"])
        with self._get_svn_fs() as svn_fs:

            if version_info.major == 2:
                ls_result = svn_fs.listdir(u"foo")
            if version_info.major == 3:
                ls_result = svn_fs.listdir("foo")

            self.assertItemsEqual(["foo_bar", "foo_baz", "bar", "baz"],
                                  ls_result)

    @_in_test_repo
    def test_open(self):
        # depends on openbin
        self.repo.add_file("foo")
        with self._get_svn_fs().open("foo") as svn_file:
            self.assertEqual("bar", svn_file.read())

    @_in_test_repo
    def test_open_fails_on_nonexistent_file(self):
        with self.assertRaises(ResourceNotFound):
            self._get_svn_fs().open("foo")

    @_in_test_repo
    def test_open_fails_on_directory(self):
        self.repo.add_dir("foo")
        with self.assertRaises(FileExpected):
            self._get_svn_fs().open("foo")

    @_in_test_repo
    def test_open_file_with_cyrillic_path(self):
        path = "doc/design/Межцентровой обмен_NEW.pdf";

        if version_info.major == 2:
            path = unicode( path );

        self.repo.add_file(path)
        with self._get_svn_fs().open(path) as svn_file:
            self.assertEqual("bar", svn_file.read())

    @_in_test_repo
    def test_open_file_with_cyrillic_content(self):
        test_fn = "Кириллица";
        
        if version_info.major == 2:
            test_fn = unicode( test_fn );

        self.repo.add_file("foo", test_fn)
        with self._get_svn_fs().open("foo") as svn_file:
            self.assertEqual(test_fn, svn_file.read())

    @_in_test_repo
    def test_getinfo_file(self):
        self.repo.add_file("foo/bar")
        info = self._get_svn_fs().getinfo("foo/bar")
        # getinfo returns basename!
        self.assertEqual("bar", info.get("basic", "name"))
        self.assertEqual(False, info.get("basic", "is_dir"))

    @_in_test_repo
    def test_getinfo_from_root(self):
        self.repo.add_file("foo/bar")
        info = self._get_svn_fs().getinfo("/foo/bar")
        self.assertEqual("bar", info.get("basic", "name"))
        self.assertEqual(False, info.get("basic", "is_dir"))

    @_in_test_repo
    def test_getinfo_dir(self):
        self.repo.add_dir("foo/bar")
        info = self._get_svn_fs().getinfo("foo/bar")
        self.assertEqual("bar", info.get("basic", "name"))
        self.assertEqual(True, info.get("basic", "is_dir"))

    @_in_test_repo
    def test_getinfo_fails_on_nonexistent_file(self):
        with self.assertRaises(ResourceNotFound):
            self._get_svn_fs().getinfo("foo")

    @_in_test_repo
    def test_copy_inside_multifs(self):
        self.repo.add_file("foo/bar")
        svn_fs=self._get_svn_fs()
        mem_fs = MemoryFS()
        root = MountFS()
        root.mount("svn", svn_fs)
        root.mount("mem", mem_fs)

        if version_info.major == 3:
            root.copy("/svn/foo/bar", "/mem/baz")
            with mem_fs.open("baz") as copied:
                self.assertEqual("bar", copied.read())

        if version_info.major == 2:
            root.copy(u"/svn/foo/bar", u"/mem/baz")
            with mem_fs.open(u"baz") as copied:
                self.assertEqual(u"bar", copied.read())

    @_in_test_repo
    def test_opendir(self):
        self.repo.add_file("foo/bar")
        top_dir = self._get_svn_fs()
        sub_dir = top_dir.opendir("foo")

        if version_info.major == 3: self.assertEqual(["bar"], sub_dir.listdir('/'))
        if version_info.major == 2: self.assertEqual(["bar"], sub_dir.listdir(u'/'))

    @_in_test_repo
    def test_copydir_inside_multifs(self):
        self.repo.add_file("foo/bar")
        svn_fs=self._get_svn_fs()
        info = svn_fs.getinfo("/foo/bar");
        root = MountFS()
        root.mount("svn", svn_fs)
        mem_fs = MemoryFS()
        root.mount("mem", mem_fs)

        if version_info.major == 3:
            root.copydir("/svn/foo", "/mem/baz", create=True)
        if version_info.major == 2:
            root.copydir(u"/svn/foo", u"/mem/baz", create=True)

        self.assertEqual(["baz"], mem_fs.listdir('/'))
        self.assertEqual(["bar"], mem_fs.listdir("baz"))

    @_in_test_repo
    def test_path_log_retrieved(self):
        svn_fs=self._get_svn_fs()
        self.repo.add_file("foo") # rev 3
        self.repo.add_file("bar/baz") # rev 4
        self.repo.add_file("bar/baz2") # rev 5
        log_entries=svn_fs.getinfo("bar", namespaces=["log"]).get("log", "change_history")
        self.assertEqual(2, len(log_entries))
        self.assertItemsEqual([{"path": "baz2", "action": "A", "copyfrom_path": None,
                                "copyfrom_revision": None} ], log_entries[0]["changed_paths"])
        self.assertEqual(5, log_entries[0]["revision"].number)
        self.assertItemsEqual([{"path": "baz", "action": "A", "copyfrom_path": None,
                                "copyfrom_revision": None},
                               {"path": "", "action": "A", "copyfrom_path": None,
                                "copyfrom_revision": None}], log_entries[1]["changed_paths"])
        self.assertEqual(4, log_entries[1]["revision"].number)

    @_in_test_repo
    def test_initial_url_log_retrieved(self):
        svn_fs=self._get_svn_fs()
        self.repo.add_file("foo") # rev 3
        self.repo.add_file("bar/baz") # rev 4
        self.repo.add_file("bar/baz2") # rev 5
        log_entries=svn_fs.getinfo('/', namespaces=["log"]).get("log", "change_history")
        self.assertEqual(5, len(log_entries)) # including 2 initial revisions
        self.assertItemsEqual([{"path": "bar/baz2", "action": "A", "copyfrom_path": None,
                                "copyfrom_revision": None} ], log_entries[0]["changed_paths"])
        self.assertEqual(5, log_entries[0]["revision"].number)
        self.assertItemsEqual([{"path": "bar/baz", "action": "A", "copyfrom_path": None,
                                "copyfrom_revision": None},
                               {"path": "bar", "action": "A", "copyfrom_path": None,
                                "copyfrom_revision": None}], log_entries[1]["changed_paths"])
        self.assertEqual(4, log_entries[1]["revision"].number)
        self.assertItemsEqual([{"path": "foo", "action": "A", "copyfrom_path": None,
                                "copyfrom_revision": None} ], log_entries[2]["changed_paths"])
        self.assertEqual(3, log_entries[2]["revision"].number)        

    @_in_test_repo
    def test_initial_url_log_limited(self):
        svn_fs=self._get_svn_fs()
        self.repo.add_file("foo") # rev 3
        self.repo.add_file("bar/baz") # rev 4
        self.repo.add_file("bar/baz2") # rev 5
        log_entries=svn_fs.getinfo('/', namespaces=["log_2"]).get("log_2", "change_history")
        self.assertEqual(2, len(log_entries))
        self.assertItemsEqual([{"path": "bar/baz2", "action": "A", "copyfrom_path": None,
                                "copyfrom_revision": None} ], log_entries[0]["changed_paths"])
        self.assertEqual(5, log_entries[0]["revision"].number)
        self.assertItemsEqual([{"path": "bar/baz", "action": "A", "copyfrom_path": None,
                                "copyfrom_revision": None},
                               {"path": "bar", "action": "A", "copyfrom_path": None,
                                "copyfrom_revision": None}], log_entries[1]["changed_paths"])
        self.assertEqual(4, log_entries[1]["revision"].number)

    @_in_test_repo
    def test_bug_creation_commit_filtered(self):
        svn_fs=self._get_svn_fs()
        log_entries=svn_fs.getinfo('/', namespaces=["log"]).get("log", "change_history")
        creation_pathes=[change["path"] for change in log_entries[-1]["changed_paths"]]
        self.assertItemsEqual(["trunk"], creation_pathes)
        
    @_in_test_repo
    def test_path_url_composed(self):
        svn_fs=self._get_svn_fs()
        syspath=svn_fs.getsyspath("dir")
        self.assertEqual(self.repo.get_path_url("dir"), syspath)

    @_in_test_repo
    def test_root_url_composed(self):
        svn_fs=self._get_svn_fs()
        syspath=svn_fs.getsyspath('/')
        self.assertEqual(self.repo.url, syspath)

    @_in_test_repo
    def test_revision_retrieved(self):
        svn_fs = self._get_svn_fs()
        info=svn_fs.getinfo('/', namespaces=["svn"])
        actual_revision=info.get("svn", "revision")
        expected_revision=2 # stub repo has two revisions initially
        self.assertEqual(expected_revision, actual_revision)

    @_in_test_repo
    def test_get_root_path_url( self ):
        svn_fs = self._get_svn_fs_ro();
        self.assertEqual( urlparse.urljoin( self.repo.url, 'buller/stroke' ), svn_fs.get_root_path_url( 'buller/stroke' ) );

        #testing unicode paths

        if version_info == 2:
            self.assertEqual( urlparse.urljoin( self.repo.url, u'Isäni/Молодец' ), svn_fs.get_root_path_url( u'Isäni/Молодец' ) );
            self.assertEqual( urlparse.urljoin( self.repo.url, u'Isäni/Молодец' ), svn_fs.get_root_path_url( unicode( 'Isäni/Молодец' ).encode( 'utf8' ) ) );

        if version_info == 3:
            self.assertEqual( urlparse.urljoin( self.repo.url, 'Isäni/Молодец' ), svn_fs.get_root_path_url( 'Isäni/'.encode( 'utf8' ) ) );

class SvnWalkTestSuite(TestCase):        
    if version_info.major == 3:
        def assertItemsEqual(self, expected_seq, actual_seq, msg=None):
            return self.assertCountEqual( actual_seq, expected_seq, msg=msg );
    def _get_svn_fs(self):
        return SvnFS(self.repo.url, pysvn.Client())

    @_in_test_repo
    def test_walk(self):
        self.repo.add_subtree("foo", ["bar1", "bar2"])
        self.repo.add_subtree("foo2", ["bar1", "bar2"])
        svn_fs = self._get_svn_fs()
        walk_result = list(svn_fs.walk())
        self.assertEqual(3, len(walk_result))
        self.assertEqual(('/', [dir_info("foo"), dir_info("foo2")], []), walk_result[0])
        self.assertEqual(("/foo", [], [file_info("bar1"), file_info("bar2")]), walk_result[1])
        self.assertEqual(("/foo2", [], [file_info("bar1"), file_info("bar2")]), walk_result[2])

    @_in_test_repo
    def test_subdir_walk(self):
        self.repo.add_subtree("foo", ["bar1", "bar2"])
        self.repo.add_subtree("foo2", ["bar1", "bar2"])
        subdir_fs = self._get_svn_fs().opendir("foo")
        walk_result=list(subdir_fs.walk())
        self.assertEqual(1, len(walk_result))
        self.assertEqual(('/', [], [file_info("bar1"), file_info("bar2")]), walk_result[0])

    @_in_test_repo
    def test_files_walked(self):
        self.repo.add_subtree("foo", ["bar1", "bar2"])
        self.repo.add_subtree("foo2", ["bar1", "bar2"])
        walk_result=list(self._get_svn_fs().walk.files())
        self.assertItemsEqual(["/foo/bar1", "/foo/bar2", "/foo2/bar1", "/foo2/bar2"],
                              walk_result)

    @_in_test_repo
    def test_dirs_walked(self):
        self.repo.add_subtree("foo", ["bar1", "bar2"])
        self.repo.add_subtree("foo2", ["bar1", "bar2"])
        walk_result=list(self._get_svn_fs().walk.dirs())
        self.assertItemsEqual(["/foo", "/foo2"], walk_result)

    @_in_test_repo
    def test_info_walk(self):
        self.repo.add_subtree("foo", ["bar1", "bar2"])
        self.repo.add_subtree("foo2", ["bar1", "bar2"])
        walk_result=list(self._get_svn_fs().walk.info())
        self.maxDiff=None
        # see https://stackoverflow.com/questions/29689606/why-does-a-successful-assertequal-not-always-imply-a-successful-assertitemsequal
        # sorting here is done by first element of tuple, so it works fine
        self.assertEqual(sorted([("/foo", dir_info("/foo")), ("/foo/bar1", file_info("/foo/bar1")),
                                 ("/foo/bar2", file_info("/foo/bar2")), ("/foo2", dir_info("/foo2")),
                                 ("/foo2/bar1", file_info("/foo2/bar1")),
                                 ("/foo2/bar2", file_info("/foo2/bar2")),]),
                         sorted(walk_result))

    @_in_test_repo
    def test_scandir(self):
        self.repo.add_subtree("foo", ["bar1", "bar2"])
        self.repo.add_subtree("foo2", ["bar1", "bar2"])
        scan_result=list(self._get_svn_fs().scandir('/'))
        # here we can't order entries since no comparable type is available
        self.assertEqual(2, len(scan_result))
        self.assertIn(dir_info("foo"), scan_result)
        self.assertIn(dir_info("foo2"), scan_result)

    @_in_test_repo
    def test_file_scandir_failure(self):
        self.repo.add_subtree("foo", ["bar1", "bar2"])
        self.repo.add_subtree("foo2", ["bar1", "bar2"])
        scanner=self._get_svn_fs().scandir("/foo/bar1")
        with self.assertRaises(DirectoryExpected):
            next(scanner)

    @_in_test_repo
    def test_scandir_empty(self):
        self.repo.add_dir("foo")
        scan_result=list(self._get_svn_fs().scandir("/foo"))
        self.assertEqual([], scan_result)
