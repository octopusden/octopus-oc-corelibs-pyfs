from fs.info import Info

# shortcuts for basic Info objects used in cdt fs implementations

basic_path_info = lambda name, is_dir: Info({ 
    "basic": {
        "name": name,
        "is_dir": is_dir
    }
})

dir_info = lambda name: basic_path_info(name, True)
file_info = lambda name: basic_path_info(name, False)
