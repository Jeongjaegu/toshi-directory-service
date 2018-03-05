from toshi.database import DatabaseMixin
from toshi.handlers import BaseHandler

DAPPS_PER_CATEGORY = 4
DEFAULT_DAPP_SEARCH_LIMIT = 10
MAX_DAPP_SEARCH_LIMIT = 100

class FrontpageHandler(DatabaseMixin, BaseHandler):

    async def get(self):
        self.write({})

class DappSearchHandler(DatabaseMixin, BaseHandler):

    async def get(self):
        self.write({})

class DappHandler(DatabaseMixin, BaseHandler):

    async def get(self, dapp_id):
        self.write({})
