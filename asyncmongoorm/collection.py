# coding: utf-8
import logging
from tornado import gen
from asyncmongoorm import bson_json
from asyncmongoorm.signal import pre_save, post_save, pre_remove, post_remove, pre_update, post_update
from asyncmongoorm.manager import Manager
from asyncmongoorm.session import Session
from asyncmongoorm.field import Field

__lazy_classes__ = {}

__collections__ = set()

def get_collections():
    return tuple(__collections__)

def register_collection(cls):
    if hasattr(cls,'__collection__'): __collections__.add(cls)

class CollectionMetaClass(type):

    def __new__(cls, name, bases, attrs):
        global __lazy_classes__
        
        # Add the document's fields to the _data
        fields = []
        for attr_name, attr_value in attrs.items():
            if hasattr(attr_value, "__class__") and issubclass(attr_value.__class__, Field):
                attr_value.name = attr_name
                fields.append(attr_value)

        new_class = super(CollectionMetaClass, cls).__new__(cls, name, bases, attrs)

        __lazy_classes__[name] = new_class
        new_class._fields = tuple(fields)
        new_class.objects = Manager(collection=new_class)
        register_collection(new_class)
        return new_class

class Collection(object):

    __metaclass__ = CollectionMetaClass

    def __new__(cls, class_name=None, *args, **kwargs):
        if class_name:
            global __lazy_classes__
            return __lazy_classes__.get(class_name)

        return super(Collection, cls).__new__(cls, *args, **kwargs)
        
    def __init__(self):
        self._data = { }
        self._changed_fields = set()

    @property
    def _field_names(self):
        return map(lambda s: s.name, self._fields)

    def as_dict(self, fields=(), exclude=(), json_compat=None):
        items = {}
        for field in self._field_names:
            if field in exclude:
                continue
            if fields and not field in fields:
                continue
            attr_value = getattr(self, field)
            items[field] = attr_value

        if json_compat:
            return bson_json.normalize(items)
        else:
            return items

    def changed_data_dict(self):
        return self.as_dict(fields=list(self._changed_fields))

    @classmethod
    def field_indexes(cls):
        indexes = []
        for attr_name, attr_type in cls.__dict__.iteritems():
            if isinstance(attr_type, Field) and attr_type.index:
                indexes.append((attr_name,
                dict( (k, True) for k in attr_type.index)))
        return indexes


    def update_attrs(self, dictionary):
        for (key, value) in dictionary.items():
            try:
                setattr(self, str(key), value)
            except TypeError, e:
                logging.warn(e)

    @classmethod
    def create(cls, dictionary):
        instance = cls()
        if not dictionary:
            return instance
        
        assert isinstance(dictionary, dict)

        if '_id' in dictionary:
            instance._is_new = False

        instance.update_attrs(dictionary)
        return instance

    def is_new(self):
        return getattr(self, '_is_new', True)

    @staticmethod
    def _handle_errors(error):
        if error and "error" in error and error["error"]:
            raise error["error"]

    @gen.engine
    def save(self, obj_data=None, callback=None):
        if self.is_new():
            yield gen.Task(pre_save.send, instance=self)
            if not obj_data:
                obj_data = self.as_dict()
            result, error = yield gen.Task(Session(self.__collection__).insert, obj_data, safe=True)
            self._handle_errors(error)
            self._is_new = False
            yield gen.Task(post_save.send, instance=self)
        else:
            yield gen.Task(pre_update.send, instance=self)

            if not obj_data:
                obj_data = self.changed_data_dict()

            response, error = yield gen.Task(Session(self.__collection__).update, {'_id': self._id}, { "$set": obj_data }, safe=True)
            self._handle_errors(error)
            yield gen.Task(post_update.send, instance=self)

        self.update_attrs(obj_data)

        if callback:
            callback(error)

    @gen.engine
    def remove(self, callback=None):
        pre_remove.send(instance=self)

        response, error = yield gen.Task(Session(self.__collection__).remove, {'_id': self._id})
        self._handle_errors(error)
        post_remove.send(instance=self)

        if callback:
            callback(error)

