from predictionserver.futureconventions.typeconventions import StrEnum
import inspect


class ApiMethod(StrEnum):
    get = 0
    post = 1
    put = 2
    delete = 3
    connect = 4
    options = 5
    trace = 6
    patch = 7


def is_api_method_name(name, selection:[ApiMethod]=None):
    """ Convention for naming class methods that will be exposed to the API """
    if selection is None:
        selection = [mthd for mthd in ApiMethod]
    has_prefix = name[:4]=='api_'
    has_suffix = any( [ name[-len(str(mthd)):]==str(mthd) for (mthd) in selection ] )
    return has_prefix and has_suffix


def select_api_method(api_obj:object, api_method:ApiMethod):
    """ A convention for selecting 'get', 'put', etc methods """
    methods = inspect.getmembers(object=api_obj, predicate=inspect.ismethod)
    selected = [m for m in methods if is_api_method_name(name=m[0], selection=[api_method])]
    if len(selected)==0:
        raise RuntimeError('Could not find get api method for class ' + str(api_obj))
    elif len(selected)>1:
        raise RuntimeError('Multiple get methods for calss ' + str(api_obj))
    return selected[0]


def api_name_from_obj(api_obj:object):
    return api_obj.__class__.__name__.replace('Api','')
