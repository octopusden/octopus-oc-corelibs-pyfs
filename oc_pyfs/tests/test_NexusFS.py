from __future__ import unicode_literals
from unittest import TestCase
from oc_cdtapi.NexusAPI import NexusAPIError
from fs.errors import ResourceNotFound, FileExpected, DirectoryExpected, Unsupported
from fs.memoryfs import MemoryFS
from fs.mountfs import MountFS
from oc_pyfs.NexusFS import NexusFS
from sys import version_info


class NexusFSTestSuite(TestCase):
    if version_info.major == 3:
        def assertItemsEqual(self, expected_seq, actual_seq, msg=None):
            return self.assertCountEqual( actual_seq, expected_seq, msg=msg );

    def _get_nexus_fs(self, *fs_args, **fs_kwargs):
        return NexusFS(*fs_args, **fs_kwargs)

    def test_open(self):
        with self._get_nexus_fs(MockCatNexusAPI()).open("g:a:v:p") as artifact:
            self.assertEqual("content", artifact.read())

    def test_open_preload(self):
        work_fs=MemoryFS()
        with self._get_nexus_fs(MockCatNexusAPI(), work_fs=work_fs).open("g:a:v:p") as artifact:
            self.assertEqual("content", artifact.read())
        listing=list(work_fs.walk.files())
        self.assertEqual(1, len(listing))
        self.assertEqual("content", work_fs.readtext(listing[0]))

    def test_nonexistent_artifact_open_failure(self):
        class MockNexusAPI(object):
            def cat(_self, gav, *args, **kwargs):
                self.assertEqual("g:a:v:p", gav)
                raise NexusAPIError(code=404)
        with self.assertRaises(ResourceNotFound):
            self._get_nexus_fs(MockNexusAPI()).open("g:a:v:p")

    def test_invalid_gav_open_failure(self):
        class MockNexusAPI(object):
            def cat(_self, gav, *args, **kwargs):
                self.assertEqual("g:a:v:p", gav)
                raise ValueError("g,a,v are mandatory")
        with self.assertRaises(FileExpected):
            self._get_nexus_fs(MockNexusAPI()).open("g:a:v:p")

    def test_copy_inside_multifs(self):
        nexus_fs= NexusFS(MockCatNexusAPI())
        mem = MemoryFS()
        root = MountFS()
        root.mount("nexus", nexus_fs)
        root.mount("mem", mem)
        root.copy(u"/nexus/g:a:v:p", u"/mem/temp.txt")
        with mem.open(u"temp.txt") as copied:
            self.assertIn(u"content", copied.read())

    def test_list_root(self):
        class MockNexusAPI(object):
            def ls(_self, wildcard, *args, **kwargs):
                self.assertEqual("com*::", wildcard)
                return ["a", "b", "c"]
        listing=self._get_nexus_fs(MockNexusAPI()).listdir("/")
        self.assertItemsEqual(["a", "b", "c"], listing)

    def test_list_dir_forbidden(self):
        with self.assertRaises(Unsupported):
            listing=self._get_nexus_fs(None).listdir("foo")

    def test_exists(self):
        class MockNexusAPI(object):
            def exists(_self, gav, *args, **kwargs):
                self.assertEqual("g:a:v:p", gav)
                return True
        nexus_fs=self._get_nexus_fs(MockNexusAPI())
        self.assertTrue(nexus_fs.exists("g:a:v:p"))

    def test_getinfo_unavailable(self):
        class MockNexusAPI(object):
            def exists(_self, gav, *args, **kwargs):
                self.assertEqual("g:a:v:p", gav)
                return True
        with self.assertRaises(Unsupported):
            self._get_nexus_fs(MockNexusAPI()).getinfo("g:a:v:p")


class MockCatNexusAPI(object):
    def cat(self_, gav, write_to=None, *args, **kwargs):
        assert "g:a:v:p" == gav
        data="content".encode("utf-8")
        if write_to:
            write_to.write(data)
        else:
            return data
