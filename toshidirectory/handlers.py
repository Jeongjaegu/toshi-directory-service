from toshi.database import DatabaseMixin
from toshi.handlers import BaseHandler
from toshi.errors import JSONHTTPError

DAPPS_PER_CATEGORY = 4
DEFAULT_DAPP_SEARCH_LIMIT = 10
MAX_DAPP_SEARCH_LIMIT = 100

async def map_dapp(dapp_id, db, dapp=None):
    if not dapp:
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

async def get_apps_by_category(category_id, db):
    dapps = []
    queried_dapps = await db.fetch('SELECT DA.dapp_id, DA.name, DA.url, DA.description, DA.icon, DA.cover FROM dapps as DA, dapp_categories as CAT ' 
                                   'WHERE DA.dapp_id = CAT.dapp_id AND category_id = $1  ORDER BY DA.name LIMIT $2' , int(category_id), DAPPS_PER_CATEGORY)
    for dapp in queried_dapps:
        mapped_app = await map_dapp(dapp['dapp_id'], db, dapp)
        dapps.append(mapped_app['dapp'])

    return dapps

async def get_apps_by_filter(db, category_id=None, query='', limit=MAX_DAPP_SEARCH_LIMIT, offset=0):
    dapps = []
    query_str = ('SELECT dapp_id, name, url, description, icon, cover FROM dapps '
                 'WHERE name ~~* $1 ORDER BY name OFFSET $2 LIMIT $3')
    query_count = ('SELECT count(dapp_id) FROM dapps WHERE name ~~* $1')

    query = '%' + query + '%'
    query_params = [query, offset, limit]
    query_count_params = [query]

    if category_id:
        query_str = ("SELECT DA.dapp_id, DA.name, Da.url, DA.description, DA.icon, DA.cover FROM dapps AS DA, dapp_categories AS CAT "
                     "WHERE CAT.category_id = $1 AND DA.dapp_id = CAT.dapp_id AND DA.name ~~* $2 ORDER BY DA.name OFFSET $3 LIMIT $4")

        query_count = ("SELECT count (DA.dapp_id) FROM dapps as DA, dapp_categories AS CAT "
                       "WHERE CAT.category_id = $1 AND DA.dapp_id = CAT.dapp_id AND DA.name ~~* $2")
        query_count_params = [int(category_id), query]

        query_params = [int(category_id), query, offset, limit]

    db_dapps = await db.fetch(query_str, *query_params)
    db_count = await db.fetch(query_count, *query_count_params)

    for db_dapp in db_dapps:
        dapp_id = db_dapp['dapp_id']
        dapp = await map_dapp(dapp_id, db, db_dapp)
        dapps.append(dapp['dapp'])

    return dapps, db_count[0]['count']

def get_categories_in_dapps(dapps):
    categories = set()
    for app in dapps:
        categories = categories.union(app['categories'])

    return list(categories)

async def get_category_names(db, categories=None):
    if categories is None:
        categories = []
    cats = await db.fetch('SELECT * from categories')
    cats = [cat for cat in cats if cat['category_id'] in categories]
    result = {}

    for cat in cats:
        result[cat['category_id']] = cat['name']

    return result

class FrontpageHandler(DatabaseMixin, BaseHandler):

    async def get(self):
        async with self.db:
            categories = await self.db.fetch('SELECT * FROM categories')
            categories_map = {}
            sections = []
            for category in categories:
                category_id = category['category_id']
                categories_map[category_id] = category['name']
                dapps = await get_apps_by_category(category_id, self.db)
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
        async with self.db:
            try:
                offset      = int(self.get_argument('offset', 0))
                limit       = min(int(self.get_argument('limit', DEFAULT_DAPP_SEARCH_LIMIT)), MAX_DAPP_SEARCH_LIMIT)
                category    = self.get_argument('category', None)
                if category:
                    category = int(category)

            except ValueError:
                raise JSONHTTPError(400, body={'errors': [{'id': 'invalid_limit_offset', 'message': 'Invalid type for limit or offset'}]})

            query = self.get_argument('query', '')

            dapps, total = await get_apps_by_filter(self.db, category, query, limit, offset)
            categories = get_categories_in_dapps(dapps)

            self.write({
                'results'   : {
                    'dapps'      : dapps,
                    'categories' : categories
                },
                'offset'    : offset,
                'limit'     : limit,
                'total'     : total,
                'query'     : query,
                'category'  : category
            })

class DappHandler(DatabaseMixin, BaseHandler):

    async def get(self, dapp_id):
        async with self.db:
           mapping = await map_dapp(dapp_id, self.db)

           self.write({
               'dapp'          : mapping['dapp'],
               'categories'    : mapping['dapp']['categories']
           })
