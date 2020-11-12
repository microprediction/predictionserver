from predictionserver.futureconventions.typeconventions import Genus
from predictionserver.futureconventions.activityconventions import Activity, ActivityContext
from predictionserver.futureconventions.memoconventions import Memo, MemoCategory, MemoConventions
from predictionserver.serverhabits.attributehabits import AttributeType, AttributeGranularity
from predictionserver.servermixins.ownershipserver import OwnershipServer
from predictionserver.servermixins.memoserver import MemoServer
from predictionserver.futureconventions.keyconventions import KeyConventions
from predictionserver.futureconventions.memoconventions import Memo
from pprint import pprint
from typing import Union


# Examples of attribute tables and keys
#
#       hash name                                                      key
#   hashattribute::performance::write_key::87a6sf876sadf87             bluestream.json|310
#   hashattribute::links::name::bluestream.json                        yellowstream.json
#   hashattribute::active::write_key:987asf8798fdsa                    yellowstream.json|310


class HashAttributeServer( MemoServer, OwnershipServer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_attribute(self, attribute: AttributeType, granularity: AttributeGranularity=None, code: str = None,
                      write_key: str = None,
                      name: str = None, delay: int = None, genus: Genus = None):
        """
               granularity is optional but strongly advised !
        """
        if granularity is None:
            granularity = self.attribute_granularity_inference(code=code, write_key=write_key, name=name, delay=delay,
                                                               genus=genus)
        return self._get_attribute_implementation(attribute=attribute, granularity=granularity, write_key=write_key,
                                                  code=code, name=name, delay=delay, genus=genus)

    def set_attribute(self, attribute: AttributeType, value: str, write_key,
                      granularity: AttributeGranularity=None, code=None, name=None, delay=None,
                      genus=None, verbose=False):
        if granularity is None:
            granularity = self.attribute_granularity_inference(code=code, name=name, delay=delay, genus=genus,
                                                               write_key=write_key)
        return self._set_attribute_implementation(attribute=attribute, granularity=granularity, value=value,
                                                  write_key=write_key, name=name, code=code, delay=delay, genus=genus,
                                                  verbose=verbose)

    def delete_attribute(self, attribute: AttributeType, write_key, granularity:AttributeGranularity=None,
                         code=None, name=None, delay=None, genus: Union[Genus, str] = None, verbose=False):
        if granularity is None:
            granularity = self.attribute_granularity_inference(code=code, name=name, delay=delay, genus=genus, write_key=write_key)
        return self._delete_attribute_implementation(attribute=attribute, granularity=granularity, write_key=write_key, name=name, code=code,
                                                     delay=delay, genus=genus, verbose=verbose)

    # ------------------
    #  Examples
    # ------------------

    def get_owner_email(self, write_key: str):
        return self.get_attribute(attribute=AttributeType.email, granularity=AttributeGranularity.write_key,
                                  write_key=write_key)

    def get_stream_description(self, name: str):
        return self.get_attribute(attribute=AttributeType.description, granularity=AttributeGranularity.name,
                                  name=name)

    def get_horizon_description(self, name: str, delay:int):
        return self.get_attribute(attribute=AttributeType.description, granularity=AttributeGranularity.name_and_delay,
                                  name=name, delay=delay)

    def get_owner_description(self, code: str):
        return self.get_attribute(attribute=AttributeType.description, granularity=AttributeGranularity.code,
                                  code=code)

    def get_owner_repository(self, code: str):
        return self.get_attribute(attribute=AttributeType.repository, granularity=AttributeGranularity.code,
                                  code=code)

    # ------------------
    #  Implementation
    # ------------------

    def _authorize_attribute_change(self, granularity: AttributeGranularity, write_key, code=None, name=None):
        if str(AttributeGranularity.code) in granularity.split():
            return self.shash(write_key) == code
        elif str(AttributeGranularity.name) in granularity.split():
            return self._authority(name) == write_key
        else:
            return KeyConventions.is_valid_key(write_key)

    def _get_attribute_implementation(self, attribute: AttributeType, granularity: AttributeGranularity,
                                      write_key, code, name, delay, genus):
        return self.execute_one(method=self.__get_attribute, attribute=attribute, granularity=granularity,
                                 write_key=write_key, code=code, name=name, delay=delay, genus=genus)

    def __get_attribute(self, pipe, attribute, granularity:AttributeGranularity, write_key, code, name, delay, genus):
        location, key = self.attribute_location_and_key(attribute=attribute, granularity=granularity,
                                                         write_key=write_key, code=code, name=name,
                                                         delay=delay, genus=genus)
        pipe.hget(name=location, key=key)
        return pipe

    def _delete_attribute_implementation(self, attribute: AttributeType, granularity: AttributeGranularity,
                                         write_key, name=None, code=None, delay=None, genus=None, verbose=False):
        allowed = self._authorize_attribute_change(granularity=granularity, write_key=write_key, name=name,
                                                   code=code)
        memo = Memo(activity=Activity.delete, context=ActivityContext.attribute, attribute=attribute,
                    write_key=write_key, allowed=int(allowed), name=name, delay=delay, genus=genus, code=code)
        if allowed:
            location, key = self.attribute_location_and_key(attribute=attribute, granularity=granularity,
                                                             write_key=write_key, name=name,
                                                             code=code, delay=delay, genus=genus)
            execution = self.client.hdel(location, key)
            assert self.add_memo_as_owner_confirm(memo=memo, success=1, execution=execution)
        else:
            assert self.add_memo_as_owner_error(memo, allowed=0)
        return memo.as_dict() if verbose else memo['success']

    def _set_attribute_implementation(self, attribute: AttributeType, granularity: AttributeGranularity, value,
                                      write_key, code=None, name=None, delay=None, genus=None, verbose=False):
        allowed = self._authorize_attribute_change(granularity=granularity, write_key=write_key, name=name,
                                                   code=code)
        memo = Memo(activity=Activity.set, context=ActivityContext.attribute, attribute=attribute, write_key=write_key,
                    allowed=int(allowed), name=name)
        if allowed:
            location, key = self.attribute_location_and_key(attribute=attribute, granularity=granularity,
                                                            write_key=write_key, name=name,
                                                             delay=delay, genus=genus)
            execution = self.client.hset(name=location, key=key, value=value)
            memo['execution'], memo['success'] = execution, execution
            assert self.add_memo_as_owner_confirm(memo, success=1, execution=execution)
        else:
            assert self.add_memo_as_owner_error(memo, allowed=0, success=0)
        return memo.as_dict() if verbose else memo['success']

    # ----------- #
    #  System     #
    # ----------- #

    def _pipe_delete_all_stream_attributes(self, pipe, names):
        """ FIXME """
        for name in names:
            for attribute in AttributeType:
                location, key = self.attribute_location_and_key(attribute=attribute,
                                                                granularity=AttributeGranularity.name,
                                                                name=name)
                pipe.hdel(location, key)
        return pipe

    def _pipe_delete_all_public_owner_attributes(self, pipe, write_key:str):
        """ FIXME """
        for attribute in AttributeType:
            location, key = self.attribute_location_and_key(attribute=attribute,
                                                            granularity=AttributeGranularity.code,
                                                            write_key=write_key,
                                                            code=self.shash(write_key))
            pipe.hdel(location, key)
        return pipe

    def _pipe_delete_all_private_owner_attributes(self, pipe, write_key: str):
        """ FIXME """
        for attribute in AttributeType:
            location, key = self.attribute_location_and_key(attribute=attribute,
                                                            granularity=AttributeGranularity.write_key,
                                                            write_key=write_key,
                                                            code=self.shash(write_key))
            pipe.hdel(location, key)
        return pipe

