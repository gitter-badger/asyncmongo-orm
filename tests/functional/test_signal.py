from copy import deepcopy
from tornado import testing, gen
from tornado.ioloop import IOLoop
from asyncmongoorm import signal
from asyncmongoorm.session import Session
from asyncmongoorm.collection import Collection
from asyncmongoorm.field import StringField, ObjectId, ObjectIdField

Session.create('localhost', 27017, 'asyncmongo_test')


class SignalTestCase(testing.AsyncTestCase):

    def get_new_ioloop(self):
        return IOLoop.instance()

    def setUp(self):
        super(SignalTestCase, self).setUp()
        SignalTestCase.signal_triggered = False
        # declare test collection mapping class
        class CollectionTest(Collection):
            __collection__ = "collection_test"
            string_attr = StringField()
        self.CollectionTest = CollectionTest

    @gen.engine
    def test_save_sends_pre_save_signal_correctly_and_I_can_handle_the_collection_instance(self):
        @signal.receiver(signal.pre_save, self.CollectionTest)
        def collection_pre_save_handler(sender, instance):
            instance.string_attr += " updated"
            SignalTestCase.signal_triggered = True

        collection_test = self.CollectionTest()
        collection_test.string_attr = "should be string value"
        yield gen.Task(collection_test.save)

        self.assertTrue(SignalTestCase.signal_triggered)

        collection_found = yield gen.Task(self.CollectionTest.objects.find_one, collection_test._id)
        self.assertEquals("should be string value updated", collection_found.string_attr)

    @gen.engine
    def test_save_sends_post_save_signal_correctly_and_I_can_handle_the_collection_instance(self):
        @signal.receiver(signal.post_save, self.CollectionTest)
        def collection_post_save_handler(sender, instance):
            self.CollectionTest.objects.find_one(collection_test._id, callback=self.stop)
            collection_found = self.wait()
            self.assertEquals(instance.string_attr, collection_found.string_attr)
            SignalTestCase.signal_triggered = True

        collection_test = self.CollectionTest()
        collection_test.string_attr = "should be string value"
        yield gen.Task(collection_test.save)
        self.assertTrue(SignalTestCase.signal_triggered)

    @gen.engine
    def test_remove_sends_pre_remove_signal_correctly_and_I_can_handle_the_collection_instance_before_it_dies(self):
        collection_test = self.CollectionTest.create(dict(string_attr = "should be string value"))
        yield gen.Task(collection_test.save)

        @signal.receiver(signal.pre_remove, self.CollectionTest)
        def collection_pre_remove_handler(sender, instance):
            SignalTestCase.instance_copy = deepcopy(instance)
            SignalTestCase.signal_triggered = True

        yield gen.Task(collection_test.remove)

        self.assertTrue(SignalTestCase.signal_triggered)
        self.assertEquals("should be string value", SignalTestCase.instance_copy.string_attr)

    @gen.engine
    def test_remove_sends_post_remove_signal_correctly_and_instance_does_not_exists_anymore(self):
        collection_test = self.CollectionTest.create(dict(string_attr="should be string value"))
        yield gen.Task(collection_test.save)

        @signal.receiver(signal.post_remove, self.CollectionTest)
        def collection_post_remove_handler(sender, instance):
            self.CollectionTest.objects.find_one(collection_test._id, callback=self.stop)
            collection_found = self.wait()
            self.assertIsNone(collection_found)
            SignalTestCase.signal_triggered = True

        collection_test.remove(callback=self.stop)

        self.wait()
        self.assertTrue(SignalTestCase.signal_triggered)

    @gen.engine
    def test_update_sends_pre_update_signal_correctly(self):
        collection_test = self.CollectionTest()
        collection_test.string_attr = "should be string value"

        yield gen.Task(collection_test.save)

        @signal.receiver(signal.pre_update, self.CollectionTest)
        def collection_pre_update_handler(sender, instance):
            instance.string_attr += ' updated'
            SignalTestCase.signal_triggered = True

        yield gen.Task(collection_test.save)

        collection_found = yield gen.Task(self.CollectionTest.objects.find_one, collection_test._id)

        self.assertEquals("should be string value updated", collection_found.string_attr)
        self.assertTrue(SignalTestCase.signal_triggered)

    @gen.engine
    def test_update_sends_post_update_signal_correctly(self):
        collection_test = self.CollectionTest()
        collection_test.string_attr = "should be string value"

        @signal.receiver(signal.post_update, self.CollectionTest)
        def collection_post_update_handler(sender, instance):
            self.assertEquals(collection_test.string_attr, instance.string_attr)
            SignalTestCase.signal_triggered = True

        yield gen.Task(collection_test.save)

        self.assertEquals("should be string value", collection_test.string_attr)
        self.assertTrue(SignalTestCase.signal_triggered)

class AsyncSignalTestCase(testing.AsyncTestCase):
    """
    Ensure that all asynchronous signal handlers working properly
    """
    def get_new_ioloop(self):
        return IOLoop.instance()

    def async_call(self, callable, *args, **kwargs):
        """
        shortcut for self.stop&self.wait pair
        """
        callable(callback=self.stop, *args, **kwargs)
        return self.wait()

    def setUp(self):
        super(AsyncSignalTestCase, self).setUp()
        AsyncSignalTestCase.signal_triggered = False
        # declare test collection mapping class
        class CollectionTest(Collection):
            __collection__ = "collection_test"
            _id = ObjectIdField(default=ObjectId)
            string_attr = StringField()
        self.CollectionTest = CollectionTest

    def test_save_sends_pre_save_signal_correctly_and_I_can_handle_the_collection_instance(self):
        @signal.receiver(signal.pre_save, self.CollectionTest)
        @gen.engine
        def collection_pre_save_handler(sender, instance, callback=None):
            instance.string_attr += " updated"
            yield gen.Task(self.CollectionTest.objects.find_one, instance._id)
            AsyncSignalTestCase.signal_triggered = True
            if callback:
                callback()
        collection_pre_save_handler.async = True
        collection_test = self.CollectionTest()
        collection_test.string_attr = "should be string value"
        self.async_call(collection_test.save)

        self.assertTrue(AsyncSignalTestCase.signal_triggered)

        collection_found = self.async_call(self.CollectionTest.objects.find_one, collection_test._id)
        self.assertEquals("should be string value updated", collection_found.string_attr)

    @gen.engine
    def test_save_sends_post_save_signal_correctly_and_I_can_handle_the_collection_instance(self):
        @signal.receiver(signal.post_save, self.CollectionTest)
        @gen.engine
        def collection_post_save_handler(sender, instance, callback=None):
            collection_found = yield gen.Task(self.CollectionTest.objects.find_one, collection_test._id)
            self.assertEquals(instance.string_attr, collection_found.string_attr)
            SignalTestCase.signal_triggered = True
            if callback:
                callback()
        collection_post_save_handler.async = True
        collection_test = self.CollectionTest()
        collection_test.string_attr = "should be string value"
        yield gen.Task(collection_test.save)
        self.assertTrue(SignalTestCase.signal_triggered)

    @gen.engine
    def test_remove_sends_pre_remove_signal_correctly_and_I_can_handle_the_collection_instance_before_it_dies(self):
        collection_test = self.CollectionTest.create(dict(string_attr = "should be string value"))
        yield gen.Task(collection_test.save)

        @signal.receiver(signal.pre_remove, self.CollectionTest)
        def collection_pre_remove_handler(sender, instance, callback=None):
            SignalTestCase.instance_copy = deepcopy(instance)
            yield gen.Task(self.CollectionTest.objects.find_one, instance._id)
            SignalTestCase.signal_triggered = True
            if callback:
                callback()
        collection_pre_remove_handler.async = True
        yield gen.Task(collection_test.remove)

        self.assertTrue(SignalTestCase.signal_triggered)
        self.assertEquals("should be string value", SignalTestCase.instance_copy.string_attr)

    @gen.engine
    def test_remove_sends_post_remove_signal_correctly_and_instance_does_not_exists_anymore(self):
        collection_test = self.CollectionTest.create(dict(string_attr="should be string value"))
        yield gen.Task(collection_test.save)

        @signal.receiver(signal.post_remove, self.CollectionTest)
        def collection_post_remove_handler(sender, instance, callback=None):
            collection_found = yield gen.Task(self.CollectionTest.objects.find_one, collection_test._id)
            self.assertIsNone(collection_found)
            SignalTestCase.signal_triggered = True
            if callback:
                callback()

        collection_post_remove_handler.async = True
        yield gen.Task(collection_test.remove)
        self.assertTrue(SignalTestCase.signal_triggered)

    def test_update_sends_pre_update_signal_correctly(self):
        collection_test = self.CollectionTest()
        collection_test.string_attr = "should be string value"

        yield gen.Task(collection_test.save)

        @signal.receiver(signal.pre_update, self.CollectionTest)
        @gen.engine
        def collection_pre_update_handler(sender, instance, callback=True):
            instance.string_attr += ' updated'
            yield gen.Task(self.CollectionTest.objects.find_one, instance._id)
            SignalTestCase.signal_triggered = True
        collection_pre_update_handler.async = True
        yield gen.Task(collection_test.save)

        collection_found = yield gen.Task(self.CollectionTest.objects.find_one, collection_test._id)
        self.assertTrue(False)
        self.assertEquals("should be string value updated", collection_found.string_attr)
        self.assertTrue(SignalTestCase.signal_triggered)

    @gen.engine
    def test_update_sends_post_update_signal_correctly(self):
        collection_test = self.CollectionTest()
        collection_test.string_attr = "should be string value"

        @signal.receiver(signal.post_update, self.CollectionTest)
        def collection_post_update_handler(sender, instance):
            self.assertEquals(collection_test.string_attr, instance.string_attr)
            SignalTestCase.signal_triggered = True

        yield gen.Task(collection_test.save)

        self.assertEquals("should be string value", collection_test.string_attr)
        self.assertTrue(SignalTestCase.signal_triggered)