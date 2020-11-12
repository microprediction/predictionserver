from predictionserver.futureconventions.apiconventions import is_api_method_name, ApiMethod, select_api_method
from predictionserver.api.attributeapi import OwnerPrivateAttributeApi


def test_is():
    assert is_api_method_name('api_blah_get')
    assert is_api_method_name('api_blah_put')
    assert is_api_method_name('api_blah_post')
    assert is_api_method_name('api_blah_patch')
    assert is_api_method_name('api_blah_delete')
    assert not is_api_method_name('apiblah_get')
    assert not is_api_method_name('api_blah_pit')
    assert not is_api_method_name('apiblah_post')
    assert not is_api_method_name('api_blah_patch_1')
    assert not is_api_method_name('api_blah_delete',selection=[ApiMethod.get])
    assert is_api_method_name('api_blah_get',selection=[ApiMethod.get])
    assert is_api_method_name('api_blah_put',selection=[ApiMethod.put, ApiMethod.get])
    assert is_api_method_name('api_blah_post')
    assert is_api_method_name('api_blah_patch')
    assert is_api_method_name('api_blah_delete')


def test_select_method():
    api = OwnerPrivateAttributeApi()
    method = select_api_method(api_obj=api, api_method=ApiMethod.get)
    assert method[0] == 'api_owner_private_attribute_get'