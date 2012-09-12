# coding: utf-8
from datetime import datetime, date
from bson import ObjectId, Binary

class Field(object):
    
    def __init__(self, default=None, name=None, field_type=None, index=None):
        
        self.default = default
        self.field_type = field_type
        self.name = name
        self.index = index
    
    def __get__(self, instance, owner):
        if not instance:
            return self
            
        value = instance._data.get(self.name)
        if value is None and self.default is not None:
            if callable(self.default):
                value = self.default()
            else:
                value = self.default
            setattr(instance, self.name, value)

        return value

    def __set__(self, instance, value):

        if value is not None and not isinstance(value, self.field_type):
            try:
                value = self.field_type(value)
            except TypeError:
                raise(TypeError("type of %s must be %s" % (self.name, self.field_type)))
            except ValueError:
                raise(TypeError("type of %s must be %s" % (self.name, self.field_type)))
        # MongoDB doesnt allow to change _id
        if self.name != "_id":
            instance._changed_fields.add(self.name)
        instance._data[self.name] = value

class StringField(Field):

    def __init__(self, *args, **kwargs):

        super(StringField, self).__init__(field_type=unicode, *args, **kwargs)
        
class IntegerField(Field):

    def __init__(self, *args, **kwargs):
        
        super(IntegerField, self).__init__(field_type=int, *args, **kwargs)

class DateTimeField(Field):

    def __init__(self, *args, **kwargs):
        
        super(DateTimeField, self).__init__(field_type=datetime, *args, **kwargs)

class DateField(Field):

    def __init__(self, *args, **kwargs):

        super(DateField, self).__init__(field_type=date, *args, **kwargs)

    def __get__(self, instance, owner):
        if not instance:
            return self

        value = instance._data.get(self.name)
        if value is None and self.default:
            if callable(self.default):
                value = self.default()
            else:
                value = self.default
            setattr(instance, self.name, value)

        return datetime(value.year, value.month, value.day)
    
class BooleanField(Field):

    def __init__(self, *args, **kwargs):

        super(BooleanField, self).__init__(field_type=bool, *args, **kwargs)

class FloatField(Field):

    def __init__(self, *args, **kwargs):

        super(FloatField, self).__init__(field_type=float, *args, **kwargs)

class ListField(Field):

    def __init__(self, *args, **kwargs):

        super(ListField, self).__init__(field_type=list, *args, **kwargs)

class ObjectField(Field):

    def __init__(self, *args, **kwargs):

        super(ObjectField, self).__init__(field_type=dict, *args, **kwargs)

class ObjectIdField(Field):

    def __init__(self, *args, **kwargs):

        super(ObjectIdField, self).__init__(field_type=ObjectId, *args, **kwargs)

class BinaryField(Field):
    def __init__(self, *args, **kwargs):

        super(BinaryField, self).__init__(field_type=Binary, *args, **kwargs)
