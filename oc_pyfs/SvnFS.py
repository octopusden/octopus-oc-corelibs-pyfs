""" This is the implementation of pyfilesystem's fs interface which wraps access to SVN repository.
Implementation is read-only, mainly because there is no obvious correspondence between svn actions like commit etc. to fs operations.
**Documentation for API methods implemented here only describes differences from default implementation. See https://docs.pyfilesystem.org for full docs.**

"""

from __future__ import unicode_literals

import io
import os
import urllib

from sys import version_info

if version_info.major == 2:
    import urlparse

import pysvn
from functools import wraps
from oc_pyfs.fs_utils import dir_info, file_info
from fs.base import FS
from fs.errors import Unsupported, ResourceNotFound, FileExpected, DirectoryExpected
from fs.info import Info
from fs.subfs import SubFS
from fs.walk import Walker, BoundWalker
from fs.wrap import WrapReadOnly
from fs.path import isbase
import logging
import traceback

logger=logging.getLogger(__name__)

def _wrap_pysvn_error(func):
    """ Maps SVN error codes to pyfs exceptions. Should be applied to all public SvnFS methods  """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except pysvn.ClientError as err:
            error_codes = [entry[1] for entry in err.args[1]]
            msg = str(err.args[0])
            if (pysvn.svn_err.fs_not_found in error_codes
                or pysvn.svn_err.ra_illegal_url in error_codes):
                raise ResourceNotFound(msg)
            elif pysvn.svn_err.client_is_directory in error_codes:
                raise FileExpected(msg)
            else:
                # exception stacktrace only includes fragment inside try block
                # full trace should be accessed manually
                full_stack_info = "\n".join(traceback.format_stack())
                logger.exception("Unhandled pysvn exception thrown. Full stacktrace: %s" % full_stack_info)
                raise Unsupported(msg)
    return wrapper


class SvnWalker(Walker):
    """ 
    Follows pyfilesystem's standard interface to traverse directory.
    Default Walker implementation uses a sequence of listdir() calls which may run quite long.
    This implementation retrieves svn recursive listing and returns it in required format.
    """

    def __init__(self, ignore_errors=False, on_error=None,
                 search="depth", filter=None, exclude_dirs=None):
        if filter or exclude_dirs:
            raise NotSupported(msg="No support for filtered walking yet")
        super(SvnWalker, self).__init__(ignore_errors, on_error, search,
                                        filter, exclude_dirs)

    @classmethod
    def bind(cls, fs):
        """  
        Overrides standard implementation which passes walker_class=Walker. This method is in fact private and should not be called by user directly.
        """
        return BoundWalker(fs, walker_class=SvnWalker)

    @_wrap_pysvn_error
    def walk(self, fs, path, namespaces=None):
        """ Only root walking is allowed for simplicity. Use fs.opendir(path).walk() instead of fs.walk(path) """
        if path != '/':
            raise Unsupported(msg="Only walking from " + '/' + " is supported. "
                              "Use opendir() to walk subdirs")
        url = fs.getsyspath(path)
        svn_client = fs.getinfo('/', ["svn"]).get("svn", "client")
        
        listing_info=self._get_listing_info(url, svn_client)
        walk_steps=self._get_walk_steps(listing_info)
        return walk_steps

    def _get_walk_steps(self, svn_pathes_info):
        """ Groups svn listing entries by directory and packs it in Info objects """
        keys_order = [ '/' ]
        dir_structure = { '/': []}
        files_structure = {'/': []}
        # ensure that entries are sorted
        for entry in sorted(svn_pathes_info, key=lambda arg: arg[0]):
            entry_path = entry[0]
            entry_dir = os.path.dirname(entry_path)
            entry_name = os.path.basename(entry_path)
            if entry[1]:  # is dir
                keys_order.append(entry_path)
                dir_structure[entry_path] = []
                files_structure[entry_path] = []
                dir_structure[entry_dir].append(entry_name)
            else:
                files_structure[entry_dir].append(entry_name)

        make_step = lambda key: (key, [dir_info(path) for path in dir_structure[key]],
                                 [file_info(path) for path in files_structure[key]])
        steps = [make_step(key) for key in keys_order]
        return steps

    def _get_listing_info(self, path, svn_client):
        """ Parses pysvn listing to intermediate (path, is_dir) tuples """
        raw_ls = svn_client.list(_get_pysvn_url(path), recurse=True,
                                 dirent_fields=pysvn.SVN_DIRENT_KIND)
        root_path = raw_ls[0][0]["repos_path"]
        actual_listing=raw_ls[1:]
        entry_path = lambda entry: entry[0]["repos_path"].replace(root_path, "", 1) or '/'
        is_entry_dir = lambda entry: entry[0]["kind"] == pysvn.node_kind.dir
        make_path_info = lambda entry: (entry_path(entry), is_entry_dir(entry))
        svn_pathes_info = [make_path_info(svn_path) for svn_path in actual_listing]
        return svn_pathes_info

    @_wrap_pysvn_error
    def info(self, fs, path='/', namespaces=None):
        """ Overrides default implementation because it calls more general walk() first. It brings unnecessary complexity when plain Info listing is needed """
        url = fs.getsyspath(path)
        svn_client = fs.getinfo('/', ["svn"]).get("svn", "client")
        listing_data=self._get_listing_info(url, svn_client)
        make_info = lambda entry: (dir_info(entry[0]) if entry[1] else file_info(entry[0]))
        listing_info=((data[0], make_info(data)) for data in listing_data)
        return listing_info

    @_wrap_pysvn_error
    def files(self, fs, path='/'):
        listing_info=self.info(fs, path)
        files=(entry[0] for entry in listing_info
               if not entry[1].is_dir)
        return files
        
    @_wrap_pysvn_error
    def dirs(self, fs, path='/'):
        listing_info=self.info(fs, path)
        dirs=(entry[0] for entry in listing_info
              if entry[1].is_dir)
        return dirs


class SvnFS(WrapReadOnly):
    """ FS standard read-only wrapper which prohibits write actions. Creates actual svn FS implementation instance internally.

    """
    walker_class=SvnWalker

    def __init__(self, branch_url, client, *args, **kwargs):
        super(SvnFS, self).__init__(SvnReadonlyFS(branch_url, client), *args, **kwargs)
    

class SvnReadonlyFS(FS):
    """ Actual SVN implementation of pyfilesystem interface.
    URLs in all pysvn calls must be wrapped with _get_pysvn_url().
    Only read-only actions are allowed.
    Methods for write actions are mocked because its existence is checked by abc.

    """

    walker_class=SvnWalker

    @_wrap_pysvn_error
    def __init__(self, branch_url, svn_client, *args, **kwargs):
        svn_client.exception_style = 1 # pass both error message and error code
        self.svn = svn_client
        self._setup_root_parameters(branch_url)
        super(SvnReadonlyFS, self).__init__(*args, **kwargs)

    def _setup_root_parameters(self, requested_url):
        """ Extracts repo root URL and base path in given repository.
        info2 allows to avoid ClientError raised by root_url_from_path when repo is relocated """
        info = self.svn.info2(_get_pysvn_url(requested_url), recurse=False)[0][1]
        branch_url=info.URL # may differ from original if redirected

        # URL of SVN repo
        self.repo_root = info.repos_root_URL.rstrip('/')

        if version_info.major == 2:
            self.repo_root = _force_unicode( self.repo_root )

        base_ls_result=self.svn.list(_get_pysvn_url(branch_url), recurse=False)[0][0]

        # path to base path relative to repo_root
        self.branch_relative=base_ls_result.repos_path.strip('/')

        if version_info.major == 2:
            self.branch_relative=_force_unicode(self.branch_relative)

    def getsyspath(self, rel_path):
        """ Overrides standard getsyspath(). 

        :param rel_path: path relative to branch url set up in __init__ 
        :returns: full URL to given path 
        """
        repo_path=_join_url(self.branch_relative, rel_path.strip('/'))
        url=self.get_root_path_url(repo_path)
        return url

    def get_root_path_url(self, repo_path):
        """ Appends given path to base repo url

        :param repo_path: Path relative to repository root 
        :returns: absolute URL of svn item
        """

        # some strings are passed as bytes in python 2
        if version_info.major == 2:
            repo_path=_force_unicode(repo_path)

        # urllib.quote doesn't process unicode strings
        # prepared_path=urllib.quote(repo_path.encode("utf8"))
        prepared_path=repo_path
        joined=_join_url(self.repo_root, prepared_path.strip('/'))

        # though non-ascii symbols are not allowed in URL, we can still present ascii symbols as unicode
        if version_info.major == 2 :
            joined=_force_unicode(joined) 
        return joined

    @_wrap_pysvn_error
    def getinfo(self, rel_path, namespaces=None):
        """ Currently supports:

        * 'basic' standard namespace

        * 'svn' custom namespace with 'client' and 'revision' keys

        * 'log*' custom namespace with 'change_history' key. Pass 'log' for full history and 'log_N' to for N latest entries
        
        """
        if version_info.major == 2:
            rel_path=_force_unicode(rel_path);
        if not namespaces:
            namespaces = ["basic"]
        if any(ns not in ["basic", "svn", "log"] and not ns.startswith("log_") for ns in namespaces):
            raise Unsupported(msg="Namespaces: " + ", ".join(namespaces) +
                              "; only basic, svn and log namespaces supported")
        full_info = {"basic": self._get_basic_info(rel_path)}
        if "svn" in namespaces:
            full_info["svn"] = self._get_svn_info(rel_path)
        log_namespaces=list(filter(lambda ns: ns.startswith("log"), namespaces))
        if log_namespaces:
            log_ns=log_namespaces[0] # use only first entry
            if log_ns=="log":
                limit=None
            else:
                limit=int(log_ns.replace("log_", ""))
            full_info[log_ns]= self._get_log_namespace(rel_path, limit)        
        return Info(full_info)

    def _get_basic_info(self, rel_path):
        rel_path = rel_path.lstrip('/')
        url = self.getsyspath(rel_path)
        pysvn_info = self.svn.info2(_get_pysvn_url(url), recurse=False)
        basic_info = {"name": os.path.basename(rel_path),
                      "is_dir": pysvn_info[0][1]["kind"] == pysvn.node_kind.dir
                      }
        return basic_info

    def _get_svn_info(self, rel_path):
        url = self.getsyspath(rel_path)
        pysvn_info = self.svn.info2(_get_pysvn_url(url), recurse=False)
        revision=pysvn_info[0][1]["rev"].number
        svn_info={"client": self.svn,
                  "revision": revision}
        return svn_info
        
    def _get_log_namespace(self, rel_path, limit=None):
        """ Retrieves log info namespace. Filters only requested entries and cuts pathes to base and requested path 

        :param rel_path: path relative to base path to retrieve logs at
        :param limit: max log entries to retrieve 
        :returns: filtered list of pysvn log entries. Pathes are cut to branch root. See pysvn docs for full description
        """
        url=self.getsyspath(rel_path)
        raw_log=self.svn.log(_get_pysvn_url(url), discover_changed_paths=True,
                             limit=limit if limit else 0)
        log_without_unrelated=[self._exclude_unrelated_changes(entry, rel_path)
                               for entry in raw_log]
        filtered_log=list( filter(lambda entry: len(entry["changed_paths"])>0,
                            log_without_unrelated) ) 
        stripped_log=[self._strip_entry_path(entry, rel_path) for entry in filtered_log]
        log_ns={"change_history": stripped_log}
        return log_ns

    def _exclude_unrelated_changes(self, entry, rel_path):
        """ Removes changed pathes which belong to same revision but not belong to requested path """
        rel_path=rel_path.strip('/')
        filtered_entry=dict(entry)
        change_url=lambda change: self.get_root_path_url(change["path"])
        requested_url=self.getsyspath(rel_path)
        is_related=lambda change: change_url(change)==requested_url or change_url(change).startswith(requested_url)
        filtered_entry["changed_paths"]=list( filter(is_related, entry["changed_paths"]) )
        return filtered_entry
        
    def _strip_entry_path(self, entry, rel_path):
        """ Strips pathes in log entry to requested path """
        rel_path=rel_path.strip('/')
        stripped_entry=dict(entry)
        strip_change_path=lambda change: self._extract_requested_path(change["path"], rel_path)
        replace_entry_path=lambda change: dict(change, path=strip_change_path(change))
        stripped_entry["changed_paths"]=map(replace_entry_path, entry["changed_paths"])
        return stripped_entry

    @_wrap_pysvn_error
    def openbin(self, rel_path, mode=u'r', buffering=-1, **options):
        """ Implemented by pysvn.cat(). File content is downloaded i.e. streaming is not supported. Only bytes read is supported """
        if mode not in ["r", "rb"]:
            raise Unsupported(msg="Only basic read mode supported at this moment")
        path = self.getsyspath(rel_path)
        file_content = self.svn.cat(_get_pysvn_url(path)) # keep it str (i.e. raw bytes)
        wrapper = io.BytesIO(file_content)
        return wrapper

    @_wrap_pysvn_error
    def listdir(self, rel_path):
        raw_ls = self._raw_svn_ls(rel_path)

        ls_from_root = [ ent[0].repos_path for ent in raw_ls ]

        if version_info.major == 2:
            ls_from_root = [ _force_unicode( ent ) for ent in ls_from_root ]

        requested_listing=[self._extract_requested_path(ls_entry, rel_path)
                           for ls_entry in ls_from_root]
        ls_result = list( filter(None, requested_listing) )
        return ls_result

    @_wrap_pysvn_error
    def scandir(self, path, namespaces=None, page=None):
        """ Only path argument is supported now """
        if namespaces or page:
            raise Unsupported("SvnFS.scandir() currently supports only path argument")
        raw_ls = self._raw_svn_ls(path)
        ls_from_root = [ent[0].repos_path for ent in raw_ls]
        node_kinds = [ent[0].kind for ent in raw_ls]
        
        requested_listing=[self._extract_requested_path(ls_entry, path)
                           for ls_entry in ls_from_root]

        rejoined = zip(requested_listing, node_kinds)
        ls = filter(lambda arg: arg[0], rejoined)

        make_info = lambda entry: (dir_info(entry[0])
                                   if entry[1] == pysvn.node_kind.dir
                                   else file_info(entry[0]))
        infos = (make_info(entry) for entry in ls)
        return infos

    def _raw_svn_ls(self, rel_path):
        """ Utility wrapper on pysvn.list. Checks that requested path is dir """
        path = self.getsyspath(rel_path)
        raw_ls = self.svn.list(_get_pysvn_url(path), recurse=False,
                               dirent_fields=pysvn.SVN_DIRENT_KIND)
        # first entry is requested dir itself
        if len(raw_ls) == 1 and raw_ls[0][0].kind == pysvn.node_kind.file:
            raise DirectoryExpected("%s is regular file" % path)
        return raw_ls[1:]

    def _extract_requested_path(self, path_from_root, branch_relative_base):
        """ Removes branch/dir path relative to root placed before requested path.
        Needed because most pysvn methods return path relative to repo root, not requested url
        :param path_from_root: path relative to repository root
        :param branch_relative_base: directory relative to base path to strip relative to
        """
        branch_relative_path=self._strip_to_base(path_from_root)
        requested_path=self._strip_dir_prefix(branch_relative_path, branch_relative_base)
        return requested_path

    def _strip_to_base(self, path_from_root):
        """ Removes branch from path from repo root """
        if version_info.major == 2:
            path_from_root = _force_unicode(path_from_root)
        cut_path=path_from_root.replace(self.branch_relative.strip('/'), "").lstrip('/')

        if version_info.major == 2:
            cut_path=_force_unicode(cut_path)

        return cut_path
        
    def _strip_dir_prefix(self, path, prefix):
        """ Removes given prefix from string. Useful to strip path to requested dir """
        # rel_path can contain leading slashes from opendir() call
        dir_name = prefix.strip('/')
        # only strip beginning, not any occurence
        if path.startswith(dir_name):
            # also remove leading slash left after dir removal
            return path.replace(dir_name, "", 1).lstrip('/') 
        else:
            return path

    # those methods are not available in read-only mode, but should still be implemented
    # because superclass checks their existence
    
    def makedir(self, path, permissions=None, recreate=False):
        """ Not supported in read-only FS"""
        raise Unsupported(msg="Read-only FS")

    def remove(self, path):
        """ Not supported in read-only FS"""
        raise Unsupported(msg="Read-only FS")

    def removedir(self, path):
        """ Not supported in read-only FS"""
        raise Unsupported(msg="Read-only FS")

    def setinfo(self, path, info):
        """ Not supported in read-only FS"""
        raise Unsupported(msg="Read-only FS")


if version_info.major == 2:
    def _force_unicode(str_data):
        """ pysvn returns strings in str, but pyfs works with unicode only """
        return str_data if isinstance(str_data, unicode) else str_data.decode("utf8")

def _join_url(*parts):
    """ due to strange behaviour of urllib.join it's better to join manually """
    return '/'.join( part.strip('/') for part in parts )


def _get_pysvn_url(url):
    """ only characters in path should be escaped, not in e.g. scheme """

    if version_info.major == 2:
        parsed_url=urlparse.urlparse(url.encode("utf8"))
        escaped_path=urllib.quote(parsed_url.path)

    if version_info.major == 3:
        parsed_url=urllib.parse.urlparse(url) # in python3 strings are always unicode - encode is not needed
        escaped_path=urllib.parse.quote(parsed_url.path)

    resulting_url_parts=list(parsed_url)
    resulting_url_parts[2]=escaped_path # 2 is index of path - see docs

    if version_info.major == 2:
        resulting_url=urlparse.urlunparse(resulting_url_parts).decode("utf8")

    if version_info.major == 3:
        resulting_url=urllib.parse.urlunparse(resulting_url_parts) # in python3 strings are unicode always, no need to decode
    return resulting_url
    
