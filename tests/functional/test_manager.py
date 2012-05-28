import unittest2

from asyncmongoorm.collection import Collection
from asyncmongoorm.session import Session
from asyncmongoorm.field import StringField, ObjectIdField

from tornado.ioloop import IOLoop
from tornado import testing, gen
from bson import ObjectId

Session.create('localhost', 27017, 'asyncmongo_test')


class CollectionTest(Collection):

    __collection__ = "collection_test"
    string_attr = StringField()
    
class ManagerTestCase(testing.AsyncTestCase):

    @gen.engine
    def tearDown(self):
        super(ManagerTestCase, self).tearDown()
        yield gen.Task(CollectionTest.objects.drop)

    def get_new_ioloop(self):
        return IOLoop.instance()

    @gen.engine
    def test_find_one(self):

        collection_test = CollectionTest()
        collection_test._id = ObjectId()
        collection_test.string_attr = "string value"
        yield gen.Task(collection_test.save)

        other_collection_test = CollectionTest()
        other_collection_test._id = ObjectId()
        other_collection_test.string_attr = "string value"
        yield gen.Task(other_collection_test.save)

        collections_found = yield gen.Task(CollectionTest.objects.find_one, {'string_attr':"string value"})

        self.assertIn(collections_found._id, (collection_test._id, other_collection_test._id))

    @gen.engine
    def test_find_one_kwargs(self):

        collection_test = CollectionTest()
        collection_test._id = ObjectId()
        collection_test.string_attr = "string value"
        yield gen.Task(collection_test.save)

        other_collection_test = CollectionTest()
        other_collection_test._id = ObjectId()
        other_collection_test.string_attr = "string value"
        yield gen.Task(other_collection_test.save)

        collections_found = yield gen.Task(CollectionTest.objects.find_one,
                {'string_attr':"string value"}, fields= {'string_attr':1})
        self.assertEqual({'string_attr':"string value"}, collections_found.as_dict())

    @gen.engine
    def test_find_one_not_found(self):
        collections_found = yield gen.Task(CollectionTest.objects.find_one, {'string_attr':"string value"})
        self.assertEquals(None, collections_found)

    @gen.engine
    def test_find(self):
        
        collection_test = CollectionTest.create(dict(string_attr = "string value"))
        yield gen.Task(collection_test.save)

        other_collection_test = CollectionTest.create(dict(string_attr = "other string value"))
        yield gen.Task(other_collection_test.save)

        collections_found = yield gen.Task(CollectionTest.objects.find, {'string_attr':"string value"})
        
        self.assertEquals(1, len(collections_found))
        self.assertEquals(collection_test._id, collections_found[0]._id)

    @gen.engine
    def test_find_not_found(self):
        collections_found = yield gen.Task(CollectionTest.objects.find, {'string_attr':"string value diff"})
        self.assertEquals([], collections_found)

    @gen.engine
    def test_count(self):

        collection_test = CollectionTest()
        collection_test._id = ObjectId()
        collection_test.string_attr = "string value"
        yield gen.Task(collection_test.save)

        count = yield gen.Task(CollectionTest.objects.count)

        self.assertEquals(1, count)
        yield gen.Task(collection_test.remove)

    @gen.engine
    def test_count_not_found(self):
        count = yield gen.Task(CollectionTest.objects.count)
        self.assertEquals(0, count)

    @gen.engine
    def test_can_be_find(self):

        yield gen.Task(CollectionTest.objects.drop)

        collection_test = CollectionTest()
        collection_test._id = ObjectId()
        collection_test.string_attr = "string value"
        yield gen.Task(collection_test.save)

        collections_found = yield gen.Task(CollectionTest.objects.find, {'string_attr':"string value"})
        self.assertEquals(collection_test._id, collections_found[0]._id)
        yield gen.Task(collection_test.remove)

    @gen.engine
    def test_find_distinct_values_with_distinct_command(self):
        collection_test = CollectionTest()
        collection_test._id = ObjectId()
        collection_test.string_attr = "Value A"
        yield gen.Task(collection_test.save)

        collection_test = CollectionTest()
        collection_test._id = ObjectId()
        collection_test.string_attr = "Value B"
        yield gen.Task(collection_test.save)

        collection_test = CollectionTest()
        collection_test._id = ObjectId()
        collection_test.string_attr = "Value A"
        yield gen.Task(collection_test.save)

        collection_test = CollectionTest()
        collection_test._id = ObjectId()
        collection_test.string_attr = "Value C"
        yield gen.Task(collection_test.save)

        distinct_values = yield gen.Task(CollectionTest.objects.distinct, key='string_attr')

        self.assertEqual(3, len(distinct_values))
        self.assertIn("Value A", distinct_values)
        self.assertIn("Value B", distinct_values)
        self.assertIn("Value C", distinct_values)

    @gen.engine
    def test_find_distinct_values_with_distinct_command_excluding_some_values(self):
        collection_test = CollectionTest()
        collection_test._id = ObjectId()
        collection_test.string_attr = "Value A"
        yield gen.Task(collection_test.save)

        collection_test = CollectionTest()
        collection_test._id = ObjectId()
        collection_test.string_attr = "Value B"
        yield gen.Task(collection_test.save)

        collection_test = CollectionTest()
        collection_test._id = ObjectId()
        collection_test.string_attr = "Value A"
        yield gen.Task(collection_test.save)

        collection_test = CollectionTest()
        collection_test._id = ObjectId()
        collection_test.string_attr = "Value C"
        yield gen.Task(collection_test.save)

        query = {
            'string_attr': {
                '$ne': 'Value A'
            }
        }
        distinct_values = yield gen.Task(CollectionTest.objects.distinct, key='string_attr', query=query)

        self.assertEqual(2, len(distinct_values))
        self.assertIn("Value B", distinct_values)
        self.assertIn("Value C", distinct_values)

    @gen.engine
    def test_execute_simple_mapreduce_return_results_inline(self):
        collections = [
            CollectionTest.create({ 'string_attr': 'Value A'}),
            CollectionTest.create({ 'string_attr': 'Value B'}),
            CollectionTest.create({ 'string_attr': 'Value A'}),
            CollectionTest.create({ 'string_attr': 'Value C'}),
            CollectionTest.create({ 'string_attr': 'Value D'}),
            CollectionTest.create({ 'string_attr': 'Value E'}),
        ]
        for coll in collections:
            yield gen.Task(coll.save)

        query = {
            'string_attr': {'$ne': 'Value E'},
        }

        map_ = """
        function m() {
            emit(this.string_attr, 1);
        }
        """

        reduce_ = """
        function r(key, values) {
            var total = 0;
            for (var i = 0; i < values.length; i++) {
                total += values[i];
            }
            return total;
        }
        """

        results = yield gen.Task(CollectionTest.objects.map_reduce, map_, reduce_, query=query)

        self.assertEquals(4, len(results))
        self.assertEquals({u'_id': u'Value A', u'value': 2.0}, results[0])
        self.assertEquals({u'_id': u'Value B', u'value': 1.0}, results[1])
        self.assertEquals({u'_id': u'Value C', u'value': 1.0}, results[2])
        self.assertEquals({u'_id': u'Value D', u'value': 1.0}, results[3])
