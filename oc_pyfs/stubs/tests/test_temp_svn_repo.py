import unittest

from sys import version_info

if version_info.major == 2:
    from urlparse import urlparse

if version_info.major == 3:
    from urllib.parse import urlparse


from oc_pyfs.stubs.temp_svn_repo import TempRepo
from pysvn import Client, ClientError


class SvnTempRepoTestSuite(unittest.TestCase):
    
    def setUp(self):
        self.svn_client=Client()
    
    def test_url_generated(self):
        with TempRepo() as temp_repo:
            url=temp_repo.get_path_url("foo/bar.txt")
            parsed_url=urlparse(url)
            self.assertEqual("file", parsed_url.scheme)
            self.assertTrue(parsed_url.path.endswith("foo/bar.txt"))
    
    def test_repo_autoremoved(self):
        with TempRepo() as temp_repo:
            root_url=temp_repo.get_path_url("")
            self.assertIsNotNone(self.svn_client.list(root_url))
        with self.assertRaises(ClientError):
            self.assertFalse(self.svn_client.list(root_url))
    
    def test_repo_autoremoved(self):
        with TempRepo() as temp_repo:
            root_url=temp_repo.get_path_url("")
            self.svn_client.list(root_url)
        with self.assertRaises(ClientError):
            self.assertFalse(self.svn_client.list(root_url))
    
    def test_dir_added(self):
        with TempRepo() as temp_repo:
            dir_url=temp_repo.get_path_url("foo")
            try:
                self.svn_client.list(dir_url)
                self.fail("Dir should not exist before mkdir call")
            except ClientError:
                temp_repo.add_dir("foo")
                self.assertEqual(1, len(self.svn_client.list(dir_url))) # dir itself
    
    def test_file_added(self):
        with TempRepo() as temp_repo:
            file_url=temp_repo.get_path_url("foo")
            try:
                self.svn_client.cat(file_url)
                self.fail("File should not exist before mkdir call")
            except ClientError:
                temp_repo.add_file("foo")

                if version_info.major == 2:
                    self.assertEqual("bar", self.svn_client.cat(file_url))

                if version_info.major == 3:
                    self.assertEqual( b"bar", self.svn_client.cat(file_url))
                    # in python3 we need to specify binary content exactly

    def test_subtree_added(self):
        with TempRepo() as temp_repo:
            tree_url=temp_repo.get_path_url("foo")
            try:
                self.svn_client.list(tree_url)
                self.fail("Dir should not exist before mkdir call")
            except ClientError:
                temp_repo.add_subtree("foo", ["bar", "baz"])
                self.assertEqual(3, len(self.svn_client.list(tree_url))) # dir with childs

                if version_info.major == 2:
                    self.assertEqual("bar", self.svn_client.cat(tree_url+"/bar"))
                    self.assertEqual("bar", self.svn_client.cat(tree_url+"/baz"))

                if version_info.major == 3:
                    self.assertEqual(b"bar", self.svn_client.cat(tree_url+"/bar"))
                    self.assertEqual(b"bar", self.svn_client.cat(tree_url+"/baz"))
