import unittest2
import fudge
from asyncmongoorm import session

class SessionTestCase(unittest2.TestCase):
    
    def setUp(self):
        session.Session._session = None

    @fudge.test
    def test_can_be_create_session(self):
        client_mock = (fudge.Fake().expects_call()
                                   .with_args(pool_id='mydb',
                                               host="should_be_host",
                                               port="should_be_port",
                                               dbname="should_be_dbname",
                                               maxclient="should_be_maxclient")
                                   .returns("shouldBeSession"))

        with fudge.patched_context(session, "Client", client_mock):
            session.Session.create(host="should_be_host",
                                   port="should_be_port",
                                   dbname="should_be_dbname",
                                   maxclient="should_be_maxclient")
        self.assertEquals(session.Session(), "shouldBeSession")
        
    def test_raise_value_error_when_session_is_not_created(self):
        
        with self.assertRaises(ValueError):
                        
            session.Session()

    @fudge.test
    def test_can_be_destroy_session(self):
        client_mock = fudge.Fake().expects("__init__").with_args(pool_id='mydb', 
                                                  host="should_be_host", 
                                                  port="should_be_port", 
                                                  dbname="should_be_dbname", 
                                                  maxclient="should_be_maxclient").returns("shouldBeSession")
                                   
        with fudge.patched_context(session, "Client", client_mock):
            session.Session.create(host="should_be_host", port="should_be_port", dbname="should_be_dbname", maxclient="should_be_maxclient")

        self.assertEquals(session.Session(), "shouldBeSession")
        
        session.Session.destroy()
        
        with self.assertRaises(ValueError):                        
            session.Session()

    @fudge.test
    def test_can_get_session_passing_collection_name_as_argumnent(self):
        session.Session._session = fudge.Fake().has_attr(collection_name='should_be_collection')
        self.assertEquals('should_be_collection', session.Session('collection_name'))

