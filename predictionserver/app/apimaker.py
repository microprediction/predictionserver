from flask_restx import reqparse, Resource
import inspect
from predictionserver.futureconventions.apiconventions import ApiMethod, select_api_method
from predictionserver.api.attributeapi import OwnerPrivateAttributeApi
from functools import partial
from flask_restx.reqparse import RequestParser
from predictionserver.futureconventions.apiconventions import api_name_from_obj
from predictionserver.serverhabits.attributehabits import attribute_docstring, AttributeGranularity
import functools

# Generate flask-application classes from classes in predictionserver/api


def generic_init(self,**kwargs):
    super().__init__(**kwargs)


def generic_api_method(parser=None,api_obj_method=None,content=None):
    if parser is not None:
        kwargs = parser.parse_args()
        return api_obj_method(**kwargs)
    elif api_obj_method is not None:
        return api_obj_method()
    else:
        return content


def make_options_api_call(content):
    return partial(generic_api_method, parser=None,api_obj_method=None,content=content)


def make_api_call(api_obj:object, api_method:ApiMethod, parser:RequestParser=None):
    """ Template
    :param api_obj:        An api server subclass implementing  api_blah_blah_get( )
    :return:               class that can be interpreted by application
    """
    api_obj_method = select_api_method(api_obj=api_obj, api_method=api_method)[1]
    mthd = partial(generic_api_method,parser=parser,api_obj_method=api_obj_method)
    return mthd


def restx_class_maker(api_obj, docstring:str, api_methods ):
    """ Generates a application friendly class with autogen docstring """

    api_name = api_name_from_obj(api_obj)
    cls_methods = {}
    options_response = {}
    for api_method in api_methods:
        parser = make_parser(api_obj,api_method=api_method)
        api_call = make_api_call(api_obj=api_obj,parser=parser, api_method=api_method)
        if parser is not None:
            payload_example = '{'+ ', '.join([ arg.name+':whatever' for arg in parser.args ])+'}'
        lower_case_method = str(api_method)
        api_call.__doc__ = lower_case_method[0].upper() + lower_case_method[1:] +\
                           ' '+ docstring
        options_response[str(api_method)] = {'description': api_call.__doc__}
        if parser is not None:
            api_call.__doc__ += '. Example payload '+payload_example
            options_response[str(api_method)].update({'parameters':[arg.name for arg in parser.args]})
        cls_methods.update({str(api_method):api_call})
    # Add options api to explain them all
    options_api_call = make_options_api_call(content=options_response)
    options_api_call.__doc__ = 'Listing of methods and arguments to supply'
    cls_methods.update({'options':options_api_call})
    return type(api_name,(Resource,),cls_methods)


def make_parser(obj, api_method: ApiMethod):
    """ Inspect object, infer which method is get, put or whatever, and create parser """
    method = select_api_method(api_obj=obj, api_method=api_method)
    full_spec = inspect.getfullargspec(method[1])
    parser = reqparse.RequestParser()
    if len(full_spec.args[1:]):
        for arg in full_spec.args[1:]:
            annotation = full_spec.annotations.get(arg)
            if annotation == str:
                parser.add_argument(arg, type='str', required=True)
            elif annotation == float:
                parser.add_argument(arg, type='float', required=True)
        return parser
    else:
        return None  # Don't want a parser


if __name__=='__main__':
    obj = OwnerPrivateAttributeApi()
    parser = make_parser(obj=obj,api_method=ApiMethod.get)
    pass
