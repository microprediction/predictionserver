import os


def devlocation()->str:
    """
       :return:  'local' or 'github
    """
    return os.getenv('DEVLOCATION') or 'local'