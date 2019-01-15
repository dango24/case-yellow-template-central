import logging
import os
import zipfile, fnmatch
import shutil, errno

def copyanything(src, dst):
    """
    Util method to recursively copy an entire directory tree rooted at src, 
    returning the destination directory. The destination directory, named by dst, must not already exist; 
    it will be created as well as missing parent directories.
    """
    if os.path.isdir(dst):
        shutil.rmtree(dst)
    elif not os.path.exists(os.path.dirname(dst)):
        # In case if it is config file(InstallerConfig.json), will create the directories to facilitate copying.
        os.makedirs(name=os.path.dirname(dst), mode=0755)
        
    try:
        shutil.copytree(src, dst)
    except OSError as exc: # python >2.5
        if exc.errno == errno.ENOTDIR:
            shutil.copy(src, dst)
        else: raise

def find_installerpkg(file_path, ext):
    """
    This helper method will find the pkg file in a given folder

    :param file_path: path to folder
    
    :return matches[0]: path to the pkg installer file.
    """
    matches = []
    files_with_ext = "*."+ext
    for root, dirnames, filenames in os.walk(file_path):
        for filename in fnmatch.filter(filenames, files_with_ext):
            matches.append(os.path.join(root, filename))
    return matches[0]
    
def extract_zip(zip_path, destination):
    """
    This helper method will extract contents of zip file in specified 
    destination directory.

    :param zip_path: path to zip file
    :type  zip_path: str

    :param destination: path to directory where zip has to be extracted (this should be an empty directory)
    :type  destination: str
    """
    
    ## creating module directory in content directory
    if not os.path.exists(destination):
        os.makedirs(destination)
    
    ## extracting zip contents 
    zip_ref = zipfile.ZipFile(zip_path, "r")
    zip_ref.extractall(destination)
    zip_ref.close()
    
def clean_directory(path, raise_on_error=False):
    """
    This helper method will remove all the contents inside a directory.
    As a safety control, this method will only operate on paths with the 
    following roots:
    
    Allowed Paths
    ==============
    /private/tmp
    /tmp
    /var/folders
    /usr/local/amazon/var
    
    .. caution: 
        This will completely delete all contents in the provided directory.
        Use with discretion.
    
    :param path: path to the directory whose contents has to be removed
    :type  path: str
    
    :param raise_on_error: If False, we will mask all exceptions
    
    :returns: True if cleanup was successful
    
    
    
    """
    ## Safety control to prevent shooting oneself in foot
    allowed_roots = [ "/private/tmp", 
                            "/tmp",
                            "/var/folders",
                            "/usr/local/amazon/var", 
                            ]
    
    logger = logging.getLogger(__name__)
    logger.debug("Cleaning directory:'{}'".format(path))
    
    
    did_succeed = False
    try:
        if not os.listdir(path):
            logger.debug("Directory '{}' is empty, no need to clean for deployment.".format(path))
            return True
        else:
            logger.debug("Cleaning directory content at path: '{}'".format(path))
        
        valid_path = False
        for root in allowed_roots:
            if path.startswith(root):
                valid_path = True
                
        if not valid_path and os.listdir(path):
            raise
        
        for root, directories, files in os.walk(path, topdown=False):
            for file in files:
                file_to_delete = os.path.join(root, file)
                os.remove(file_to_delete)
            for directory in directories:
                os.rmdir(os.path.join(root, directory))
        os.rmdir(path)
        did_succeed = True
    except Exception as exp:
        if raise_on_error:
            raise
        else:
            logger.error("Failed to cleanup directory:'{}'; {}".format(
                                                            exp.message))
            logger.log(5,"Failure stack trace (handled cleanly)", 
                                                    exc_info=1)
          
    return did_succeed