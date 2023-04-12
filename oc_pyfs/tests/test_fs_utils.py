from oc_pyfs.fs_utils import basic_path_info, dir_info, file_info
from unittest import TestCase
from fs.info import Info

class TestFsUtils( TestCase ):
    def test_basic_path_info( self ):
        self.assertEqual( basic_path_info( "lazhaa", False ), Info( { 
            "basic": {
                "name": "lazhaa",
                "is_dir": False
            } } ) )
        self.assertEqual( basic_path_info( "baatvaa", True ), Info( { 
            "basic": {
                "name": "baatvaa",
                "is_dir": True
            } } ) )

    def test_file_info( self ):
        self.assertEqual( file_info( "lazhaa" ), Info( { 
            "basic": {
                "name": "lazhaa",
                "is_dir": False
            } } ) )

    def test_dir_info( self ):
        self.assertEqual( dir_info( "baatvaa" ), Info( { 
            "basic": {
                "name": "baatvaa",
                "is_dir": True
            } } ) )

