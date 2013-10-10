from asyncmongo import Client

class Session(object):
    
    _session = None
    _pool_id = "mydb"
    def __new__(cls, collection_name=None):

        if not cls._session:
            raise ValueError("Session is not created")
        if collection_name:
            return getattr(cls._session, collection_name)
            
        return cls._session
        
    @classmethod
    def create(cls, host, port, dbname, **kwargs):
        if not cls._session:
            cls._session = Client(pool_id=cls._pool_id, host=host, port=port, dbname=dbname, **kwargs)
    
    @classmethod
    def destroy(cls):
        cls._session._pool.close()
        cls._session = None