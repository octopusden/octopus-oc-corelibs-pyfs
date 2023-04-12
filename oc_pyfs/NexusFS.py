""" This is the implementation of pyfilesystem's fs interface which wraps access to Nexus repository.
Implementation is currently read-only. No directories are allowed in pathes; you may think of repository as large top-level directory with all available artifacts in it.
**Documentation for API methods implemented here only describes differences from default implementation. See https://docs.pyfilesystem.org for full docs.**

"""
from __future__ import unicode_literals

import io
import logging
from oc_cdtapi import NexusAPI
from uuid import uuid4
from functools import wraps
from fs.base import FS
from fs.errors import Unsupported, ResourceNotFound, FileExpected, DirectoryExpected
from fs.info import Info
from fs.wrap import WrapReadOnly
from sys import version_info


logger=logging.getLogger(__name__)


def _wrap_nexusapi_error(fun):
    """ Maps HTTP-based NexusAPI errors to fs exceptions. All methods calling NexusAPI should be wrapped in this. """
    @wraps(fun)
    def wrapped(*args, **kwargs):
        try:
            result = fun(*args, **kwargs)
            return result
        except NexusAPI.NexusAPIError as err:
            logger.exception("NexusAPI error occured")
            if (err.code == 404):
                raise ResourceNotFound("Cannot find artifact: "+str(err))
            else:
                raise Unsupported("Unknown error: %s" % str(err))
        except ValueError as err:
            logger.exception("ValueError occured during NexusAPI call")
            if "mandatory" in str(err):
                raise FileExpected("Please specify full GAV: "+str(err))
            else:
                raise
    return wrapped


class NexusFS(WrapReadOnly):
    """ FS standard read-only wrapper which prohibits write actions. Creates actual Nexus FS implementation instance internally.

    """
    
    def __init__(self, client, work_fs=None, *args, **kwargs):
        super(NexusFS, self).__init__(NexusReadonlyFS(client, work_fs), *args, **kwargs)


class NexusReadonlyFS(FS):
    def __init__(self, nexus_client, work_fs=None, *args, **kwargs):
        """
        :param nexus_client: NexusAPI client 
        :param work_fs: optional FS instance where loaded artifacts will be cached (reduces RAM consumption). Should be cleaned by user """
        self._nexus = nexus_client
        self._work_fs=work_fs
        super(NexusReadonlyFS, self).__init__(*args, **kwargs)

    def getinfo(self, path, namespaces=None):
        """ **Not implemented yet** """
        raise Unsupported("WIP")

    @_wrap_nexusapi_error
    def openbin(self, gav, mode="r", buffering=-1, **options):
        """ Retrieves artifact content. Only binary read is supported now. Other params are ignored.
        If work_fs was specified in constructor, then artifact is loaded into it, and then handle to work_fs file is returned.
        
        :param gav: gav of artifact (acts like regular filename)
        :returns: file-like object pointing to artifact content
        """
        if mode not in ["r", "rb"]:
            raise Unsupported("Only basic read mode supported at this moment")
        if self._work_fs:
            if version_info.major == 2:
                preload_filename=unicode(uuid4())

            if version_info.major == 3:
                preload_filename=str(uuid4())

            with self._work_fs.openbin(preload_filename, "w") as preload_file:
                self._nexus.cat(gav, stream=True, write_to=preload_file)
            handle=self._work_fs.openbin(preload_filename)
        else:
            file_content = self._nexus.cat(gav, binary=True)
            handle = io.BytesIO(file_content)
        return handle

    @_wrap_nexusapi_error
    def listdir(self, path):
        """ Returns list of artifacts in repository. **Unstable NexusAPI.ls is used, so listing may be incomplete**. 
        
        :param path: should always be '/'. Listing anything else doesn't make sense because currently NexusFS' represents repository as one large folder
        :returns: list of gavs
        """
        logger.warning("NexusFS.listdir relies on unstable NexusAPI.ls."
                        " Be careful using it")
        if path != u"/":
            raise Unsupported("Only root listing makes sense for Nexus repository")
        all_artifacts_wildcard = "com*::"
        return self._nexus.ls(all_artifacts_wildcard)

    @_wrap_nexusapi_error
    def exists(self, path):
        return self._nexus.exists(path)

    # those methods are not available in read-only mode, but should still be implemented
    # because superclass checks their existence
    
    def makedir(self, path, permissions=None, recreate=False):
        """ Not supported in read-only FS """
        raise Unsupported("Read-only FS")

    def remove(self, path):
        """ Not supported in read-only FS """
        raise Unsupported("Read-only FS")

    def removedir(self, path):
        """ Not supported in read-only FS """
        raise Unsupported("Read-only FS")

    def setinfo(self, path, info):
        """ Not supported in read-only FS """
        raise Unsupported("Read-only FS")

