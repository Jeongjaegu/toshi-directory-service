from toshi.database import DatabaseMixin
from toshi.handlers import BaseHandler
from toshi.errors import JSONHTTPError

DAPPS_PER_CATEGORY = 4
DEFAULT_DAPP_SEARCH_LIMIT = 10
MAX_DAPP_SEARCH_LIMIT = 100

async def map_dapp(dapp_id, db):
    dapp = await db.fetchrow('SELECT * FROM dapps WHERE dapp_id = $1', int(dapp_id))
    if not dapp:
        raise JSONHTTPError(400, body={'errors': [{'id': 'invalid_dapp_id', 'message': 'Invalid Dapp Id'}]})

    categories = await db.fetch('SELECT category_id FROM dapp_categories WHERE dapp_id = $1', int(dapp_id))
    categories = [category['category_id'] for category in categories]

    return {
        'dapp'  :   {
            'dapp_id'       : int(dapp_id),
            'name'          : dapp['name'],
            'url'           : dapp['url'],
            'description'   : dapp['description'],
            'icon'          : dapp['icon'],
            'cover'         : dapp['cover'],
            'categories'    : categories
        }
    }

class FrontpageHandler(DatabaseMixin, BaseHandler):

    async def get_apps_by_category(self, category_id):
        dapps = []
        dapps_ids = await self.db.fetch('SELECT dapp_id FROM dapp_categories WHERE category_id = $1', int(category_id))
        for dapp in dapps_ids:
            mapped_app = await map_dapp(dapp['dapp_id'], self.db)
            dapps.append(mapped_app['dapp'])
        return dapps


    async def get(self):
        async with self.db:
            categories = await self.db.fetch('SELECT * FROM categories')
            categories_map = {}
            sections = []
            for category in categories:
                category_id = category['category_id']
                categories_map[category_id] = category['name']
                dapps = await self.get_apps_by_category(category_id)
                dapps = sorted(dapps, key = lambda e : e['name'])[:DAPPS_PER_CATEGORY]
                sections.append({
                     'category_id' : category_id,
                     'dapps'       : dapps
                })

            self.write({
                'sections'   : sections,
                'categories' : categories_map
            })

class DappSearchHandler(DatabaseMixin, BaseHandler):

    async def get(self):
        self.write({})

class DappHandler(DatabaseMixin, BaseHandler):

    async def get(self, dapp_id):
        async with self.db:
           mapping = await map_dapp(dapp_id, self.db)

           self.write({
               'dapp'          : mapping['dapp'],
               'categories'    : mapping['dapp']['categories']
           })
                
