from __future__ import unicode_literals
from sys import version_info
import os
from fs.tempfs import TempFS
import tarfile
import pysvn
import urllib
from pkg_resources import Requirement, resource_filename

# path is resolved accordingly to installed package's path
_empty_repo_path=resource_filename("oc_pyfs.stubs", "empty_svn_repo.tar.gz")


class TempRepo(object):
    """ Provides local SVN repository with some helper methods to setup it.
    Intended to be used as context manager so repository is destroyed after usage. 
    Repository is initialized from archive at _empty_repo_path.
    No trunk/branches/etc. structure is prepared - just empty directory.
    """

    def __enter__(self):
        self._temp_fs=TempFS()
        self._svn_client=pysvn.Client()
        with tarfile.open(_empty_repo_path, "r:gz") as repo_archive:
            repo_archive.extractall(self._temp_fs.getsyspath('/'))
        return self

    @property
    def url(self):
        """ SVN URL of repository root """
        return self.get_path_url("/")    

    def get_path_url(self, path):
        """ Converts path in repository into absolute SVN url 

        :param path: path relative to repository root 
        :returns: URL of given path
        """
        path=path.strip("/")
        template="file://%s/%s"
        return template % (self._temp_fs.getsyspath('/').rstrip('/'), path)

    def __exit__(self, *args):
        self._temp_fs.close()

    def add_dir(self, path):
        """ Creates empty directory (with parents if necessary) 

        :param path: path to new directory inside repository 
        """
        full_url=self.get_path_url(path)
        self._svn_client.mkdir(full_url, make_parents=True,
                              log_message="test addition")
    
    def add_file(self, path, content="bar"):
        """ Creates file in repository.

        :param path: path to new file inside repository 
        :param content: optional file content 
        """
        full_url=self.get_path_url(path)
        with TempFS() as creation_fs:
            creation_dir=creation_fs.getsyspath('/').rstrip('/')
            creation_fs.writetext("foo", content)
            target_url="file://"

            if version_info.major == 2:
                prepared_url_wo_scheme=urllib.quote(full_url.replace("file://", "").encode("utf8"))
                target_url=target_url.encode("utf8")+prepared_url_wo_scheme
                source_path=os.path.join(creation_dir, "foo")#.encode("utf8")
                self._svn_client.import_(source_path, target_url,
                                     log_message="test addition".encode("utf8"))

            if version_info.major ==3:
                prepared_url_wo_scheme=urllib.parse.quote(full_url.replace("file://", ""))
                target_url=target_url+prepared_url_wo_scheme
                # in python3 strings are always unicode, so encoding is not necessary
                source_path=os.path.join(creation_dir, "foo")#.encode("utf8")
                self._svn_client.import_(source_path, target_url,
                                     log_message="test addition")

    
    def add_subtree(self, dir_path, file_pathes):
        """ Shortcut for preparing directory structure for case whe files content doesn't matter. 

        :param dir_path: path to new directory
        :param file_pathes: list of files to be created inside new directory. Default value for add_file is written into them.
        """
        self.add_dir(dir_path)
        for file_path in file_pathes:
            full_file_path="/".join([dir_path, file_path])
            self.add_file(full_file_path)
