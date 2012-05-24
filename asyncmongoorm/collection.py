# coding: utf-8
import functools
import logging
from bson.objectid import ObjectId
from tornado import gen
from asyncmongoorm.signal import pre_save, post_save, pre_remove, post_remove, pre_update, post_update
from asyncmongoorm.manager import Manager
from asyncmongoorm.session import Session
from asyncmongoorm.field import Field, ObjectIdField

__lazy_classes__ = {}

class CollectionMetaClass(type):

    def __new__(cls, name, bases, attrs):
        global __lazy_classes__
        
        # Add the document's fields to the _data
        for attr_name, attr_value in attrs.items():
            if hasattr(attr_value, "__class__") and issubclass(attr_value.__class__, Field):
                attr_value.name = attr_name
                
        new_class = super(CollectionMetaClass, cls).__new__(cls, name, bases, attrs)

        __lazy_classes__[name] = new_class
        
        new_class.objects = Manager(collection=new_class)
        
        return new_class

class BaseCollection(object):

    __metaclass__ = CollectionMetaClass

    def __new__(cls, class_name=None, *args, **kwargs):
        if class_name:
            global __lazy_classes__
            return __lazy_classes__.get(class_name)

        return super(BaseCollection, cls).__new__(cls, *args, **kwargs)
        
    def __init__(self):
        self._data = {}
        self._changed_fields = set()

    def as_dict(self, fields=(), exclude=()):
        items = {}
        for attr_name, attr_type in self.__class__.__dict__.iteritems():
            if attr_name in exclude:
                continue
            if fields and not attr_name in fields:
                continue
            if isinstance(attr_type, Field):
                attr_value = getattr(self, attr_name)
                if attr_value != None:
                    items[attr_name] = attr_value
        return items

    def changed_data_dict(self):
        return self.as_dict(fields=list(self._changed_fields))

    @classmethod
    @gen.engine
    def setup_indexes(self):
        raise NotImplemented

    @classmethod
    def create(cls, dictionary):
        instance = cls()
        if '_id' in dictionary:
            instance._is_new = False
        for (key, value) in dictionary.items():
            try:
                setattr(instance, str(key), value)
            except TypeError, e:
                logging.warn(e)

        return instance

    def is_new(self):
        return getattr(self, '_is_new', True)

    @gen.engine
    def save(self, obj_data=None, callback=None):
        if self.is_new():
            pre_save.send(instance=self)

            result, error = yield gen.Task(Session(self.__collection__).insert, self.as_dict(), safe=True)
            self._is_new = False

            post_save.send(instance=self)
        else:
            pre_update.send(instance=self)

            if not obj_data:
                obj_data = self.changed_data_dict()

            response, error = yield gen.Task(Session(self.__collection__).update, {'_id': self._id}, { "$set": obj_data }, safe=True)

            post_update.send(instance=self)

        if callback:
            callback(error)

    @gen.engine
    def remove(self, callback=None):
        pre_remove.send(instance=self)

        response, error = yield gen.Task(Session(self.__collection__).remove, {'_id': self._id})

        post_remove.send(instance=self)

        if callback:
            callback(error)


class Collection(BaseCollection):
    _id = ObjectIdField(default=lambda : ObjectId())