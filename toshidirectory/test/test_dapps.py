import datetime
import sys
import os

from tornado.escape import json_decode
from tornado.testing import gen_test

from toshi.test.base import AsyncHandlerTest
from toshi.test.database import requires_database

from toshidirectory.app import urls
from toshidirectory.handlers import DAPPS_PER_CATEGORY, DEFAULT_DAPP_SEARCH_LIMIT, MAX_DAPP_SEARCH_LIMIT

TEST_DAPP_DATA = [
    (1673246598739526658,
     'Cent',
     'https://beta.cent.co/',
     'Give wisdom, get money. Ask a question and offer a bounty for the best answers. The userbase then votes to determine which answers receive that bounty.',
     'https://www.toshi.org/0x000000000000000000000000173890ea93800402_613d88.png',
     datetime.datetime(2017, 12, 19, 12, 29, 20, 552493),
     datetime.datetime(2017, 12, 19, 12, 29, 20, 552493),
     1),
    (1673246774900294659,
     'NameBazaar',
     'https://namebazaar.io/',
     'A peer-to-peer marketplace for the exchange of names registered via the Ethereum Name Service.',
     'https://www.toshi.org/0x0000000000000000000000001738911397800403_a64bad.png',
     datetime.datetime(2017, 12, 19, 12, 29, 41, 692435),
     datetime.datetime(2017, 12, 19, 12, 29, 41, 692435),
     2),
    (1673246900729414660,
     'Cryptokitties',
     'https://www.cryptokitties.co/',
     'CryptoKitties is a game centered around breedable, collectible, and oh-so-adorable creatures we call CryptoKitties! Each cat is one-of-a-kind and 100% owned by you; it cannot be replicated, taken away, or destroyed.',
     'https://www.toshi.org/0x00000000000000000000000017389130e3800404_3ac912.png',
     datetime.datetime(2017, 12, 19, 12, 29, 56, 273100),
     datetime.datetime(2017, 12, 19, 12, 29, 56, 273100),
     3),
    (1674816702887494661,
     'Ethlance',
     'https://ethlance.com/',
     'The future of work is now! Hire or work for Ether cryptocurrency',
     'https://www.toshi.org/0x000000000000000000000000173e24eaef800405_f1b0b7.png',
     datetime.datetime(2017, 12, 21, 16, 28, 51, 564216),
     datetime.datetime(2017, 12, 21, 16, 28, 51, 564216),
     1),
    (1674820117050950662,
     'Leeroy',
     'https://leeroy.io/',
     'Leeroy is a decentralised social network built on Ethereum.',
     'https://www.toshi.org/0x000000000000000000000000173e2805db800406_7ad6b4.png',
     datetime.datetime(2017, 12, 21, 16, 35, 38, 270798),
     datetime.datetime(2017, 12, 21, 16, 35, 38, 270798),
     2),
    (1674821140461126663,
     'CryptoPunks',
     'https://www.larvalabs.com/cryptopunks',
     '10,000 unique collectible characters with proof of ownership stored on the Ethereum blockchain.',
     'https://www.toshi.org/0x000000000000000000000000173e28f423800407_c9007c.jpg',
     datetime.datetime(2017, 12, 21, 16, 37, 40, 235798),
     datetime.datetime(2017, 12, 21, 16, 37, 40, 235798),
     3),
    (1695885715289670663,
     'Crypto High Score',
     'https://www.cryptohighscore.co',
     'The first high score leaderboard on the blockchain. Pay your way to the top of the immutable, irrefutable global rankings.',
     'https://www.toshi.org/0x0000000000000000000000001788ff12a7800407_ca5ba0.png',
     datetime.datetime(2018, 1, 19, 18, 9, 13, 815644),
     datetime.datetime(2018, 1, 19, 18, 9, 13, 815644),
     1),
    (154552072,
     'ERC dEX',
     'https://app.ercdex.com/',
     'Trustless\xa0trading has arrived on Ethereum. No need to deposit your tokens or create an account - trade ERC20 tokens directly from your wallet.',
     'https://www.toshi.org/0x0000000000000000000000001795100a0b800408_543c99.png',
     datetime.datetime(2018, 1, 24, 10, 37, 10, 994011),
     datetime.datetime(2018, 1, 24, 10, 37, 10, 994011),
     2),
    (1704426417141318665,
     'ChainMonsters',
     'https://chainmonsters.io/',
     'ChainMonsters is a 100% blockchain based monster collectible game. Every action you take, every ChainMonster you catch will be reflected in the game and on blockchain itself.',
     'https://www.toshi.org/0x00000000000000000000000017a756cbc3800409_393169.png',
     datetime.datetime(2018, 1, 31, 12, 58, 4, 697837),
     datetime.datetime(2018, 1, 31, 12, 58, 4, 697837),
     2),
    (1708300997736006666,
     'Etheremon',
     'https://www.etheremon.com/',
     'A decentralized application built on the Ethereum network to simulate a world of monsters where you can capture, evolve a monster to defeat others.',
     'https://www.toshi.org/0x00000000000000000000000017b51ab4db80040a_00dfcb.png',
     datetime.datetime(2018, 2, 5, 21, 16, 10, 638845),
     datetime.datetime(2018, 2, 5, 21, 16, 10, 638845),
     3),
    (1708333788804678667,
     'Ethercraft',
     'https://ethercraft.io/',
     'A decentralized RPG running on the Ethereum blockchain.',
     'https://www.toshi.org/0x00000000000000000000000017b538879f80040b_944085.png',
     datetime.datetime(2018, 2, 5, 22, 21, 19, 564400),
     datetime.datetime(2018, 2, 5, 22, 21, 19, 564400),
     4),
    (1708349878154822668,
     'Etherbots',
     'https://etherbots.io/',
     'A decentralized Robot Wars game for the Ethereum blockchain.',
     'https://www.toshi.org/0x00000000000000000000000017b54729b780040c_ecda6e.jpg',
     datetime.datetime(2018, 2, 5, 22, 53, 18, 15577),
     datetime.datetime(2018, 2, 5, 22, 53, 18, 15577),
     4),
    (1308366948,
     'Hong Bao',
     'https://givehongbao.com/',
     'Crowdfunded campaigns powered by the Ethereum Network. Hong Bao lets you raise money for any cause, anywhere in the world.',
     'https://www.toshi.org/0x00000000000000000000000017b548dd4380040d_c823c8.jpg',
     datetime.datetime(2018, 2, 5, 22, 57, 0, 946397),
     datetime.datetime(2018, 2, 5, 22, 57, 0, 946397),
     5),
    (1708352654784070670,
     'World of Ether',
     'https://worldofether.com/',
     'Fully decentralized collectable duelling game on the Ethereum blockchain. Collect, breed, battle.',
     'https://www.toshi.org/0x00000000000000000000000017b549b03380040e_b6b3c8.jpg',
     datetime.datetime(2018, 2, 5, 22, 58, 48, 948025),
     datetime.datetime(2018, 2, 5, 22, 58, 48, 948025),
     6),
    (1710548330985030671,
     'CryptoFighters',
     'https://cryptofighters.io/',
     'CryptoFighters is a game centred around cryptographically unique collectible fighters on the Ethereum blockchain. Collect, battle and level up your fighters to win new fighters!',
     'https://www.toshi.org/0x00000000000000000000000017bd16a4e780040f_0bb1e3.png',
     datetime.datetime(2018, 2, 8, 23, 41, 13, 585866),
     datetime.datetime(2018, 2, 8, 23, 41, 13, 585866),
     7),
    (1710550109369926672,
     'Crypto Celebrities',
     'https://www.cryptocelebrities.co/',
     'Collect one-of-a-kind celebrity smart contracts',
     'https://www.toshi.org/0x00000000000000000000000017bd1842f7800410_73effb.png',
     datetime.datetime(2018, 2, 8, 23, 44, 45, 829020),
     datetime.datetime(2018, 2, 8, 23, 44, 45, 829020),
     8),
    (1710555771680326675,
     'Ether Numbers',
     'https://ethernumbers.co/',
     'EtherNumbers is a marketplace for collectible, tokenized representations of your favorite numbers. Each EtherNumber is like a baseball card, except there can only ever be one!',
     'https://www.toshi.org/0x00000000000000000000000017bd1d6953800413_e028d0.jpg',
     datetime.datetime(2018, 2, 8, 23, 56, 0, 959724),
     datetime.datetime(2018, 2, 8, 23, 56, 0, 959724),
     1),
    (1710556308551238676,
     'OpenSea',
     'https://opensea.io/',
     'Peer-to-peer marketplace for rare digital items.',
     'https://www.toshi.org/0x00000000000000000000000017bd1de653800414_308bf8.png',
     datetime.datetime(2018, 2, 8, 23, 57, 4, 888989),
     datetime.datetime(2018, 2, 8, 23, 57, 4, 888989),
     2),
    (1713827119085126677,
     'Rare Bits',
     'https://rarebits.io/',
     'A zero fee marketplace for crypto assets.',
     'https://www.toshi.org/0x00000000000000000000000017c8bcaf3f800415_2ff16a.png',
     datetime.datetime(2018, 2, 13, 12, 15, 35, 882713),
     datetime.datetime(2018, 2, 13, 12, 15, 35, 882713),
     3),
    (1715336347104838676,
     'Crypto Speech',
     'http://cryptospeech.com/',
     'Decentralized, Twitter-style messages stored for eternity',
     'https://www.toshi.org/0x00000000000000000000000017ce1951c7800414_2649b0.png',
     datetime.datetime(2018, 2, 15, 14, 14, 9, 986518),
     datetime.datetime(2018, 2, 15, 14, 14, 9, 986518),
     4),
    (1721937401460294677,
     'Kpopio',
     'https://www.kpop.io/',
     'Kpopio is an online game where you can buy and sell Kpop celebrity cards with other players.',
     'https://www.toshi.org/0x00000000000000000000000017e58cf183800415_3ba268.png',
     datetime.datetime(2018, 2, 24, 16, 49, 16, 855046),
     datetime.datetime(2018, 2, 24, 16, 49, 16, 855046),
     5),
    (1721937401460294689,
     'ZZZZZZZZZZ',
     'https://www.kpoptwo.io/',
     'Kpopio2 is an online game where you can buy and sell Kpop celebrity cards with other players.',
     'https://www.toshi.org/0x00000000000000000000000kop2.png',
     datetime.datetime(2018, 2, 24, 13, 49, 16, 855046),
     datetime.datetime(2018, 2, 24, 13, 49, 16, 855046),
     6),
    (1700000000000000001,
     'Test Dapp 1',
     'https://www.test.dapp/',
     'A test Dapp',
     'https://www.toshi.org/testdap2.png',
     datetime.datetime(2018, 2, 24, 13, 49, 16, 855046),
     datetime.datetime(2018, 2, 24, 13, 49, 16, 855046),
     0),
    (1700000000000000002,
     'Test Dapp 2',
     'https://www.test.dapp/',
     'A test Dapp',
     'https://www.toshi.org/testdap2.png',
     datetime.datetime(2018, 2, 24, 13, 49, 16, 855046),
     datetime.datetime(2018, 2, 24, 13, 49, 16, 855046),
     0),
    (1700000000000000003,
     'Test Dapp 3',
     'https://www.test.dapp/',
     'A test Dapp',
     'https://www.toshi.org/testdap2.png',
     datetime.datetime(2018, 2, 24, 13, 49, 16, 855046),
     datetime.datetime(2018, 2, 24, 13, 49, 16, 855046),
     0),
    (1700000000000000004,
     'Test Dapp 4',
     'https://www.test.dapp/',
     'A test Dapp',
     'https://www.toshi.org/testdap2.png',
     datetime.datetime(2018, 2, 24, 13, 49, 16, 855046),
     datetime.datetime(2018, 2, 24, 13, 49, 16, 855046),
     0),
]

TEST_CATEGORY_DATA = [
    (1, "Games & Collectibles"),
    (2, "Marketplaces"),
    (3, "Jobs"),
    (4, "Social Media"),
    (5, "Crowdfunding"),
    (6, "Exchanges"),
    (7, "Testing")
]

TEST_DAPP_CATEGORY_DATA = [
    (1673246598739526658, 4),
    (1673246774900294659, 2),
    (1673246900729414660, 1),
    (1674816702887494661, 2),
    (1674816702887494661, 3),
    (1674820117050950662, 4),
    (1674821140461126663, 1),
    (1695885715289670663, 1),
    (154552072, 6),
    (1704426417141318665, 1),
    (1708300997736006666, 1),
    (1708333788804678667, 1),
    (1708349878154822668, 1),
    (1308366948, 5),
    (1708352654784070670, 1),
    (1710548330985030671, 1),
    (1710550109369926672, 1),
    (1710555771680326675, 1),
    (1710556308551238676, 1),
    (1710556308551238676, 2),
    (1713827119085126677, 1),
    (1713827119085126677, 2),
    (1715336347104838676, 4),
    (1721937401460294677, 1),
    (1700000000000000001, 7),
    (1700000000000000002, 7),
    (1700000000000000003, 7),
    (1700000000000000004, 7),
]

TEST_IOS_HIDDEN_DAPPS = [
    1673246598739526658,
    1673246774900294659
]

TEST_ANDROID_HIDDEN_DAPPS = [
    1674816702887494661,
    1674820117050950662,
]

TEST_ALL_HIDDEN_DAPPS = [
    154552072,
    1308366948
]

TEST_IOS_HIDDEN_CATEGORIES = [
    2
]

TEST_ANDROID_HIDDEN_CATEGORIES = [
    3
]

TEST_ALL_HIDDEN_CATEGORIES = [
    4
]

TEST_IOS_USER_AGENT = "Toshi/500 testing"
TEST_ANDROID_USER_AGENT = "Android test build:500"
TEST_UNKNOWN_USER_AGENT = "unknown"

def dapp_only_in_categories(dapp_id, category_ids):
    categories = [cat_id for d_id, cat_id in TEST_DAPP_CATEGORY_DATA if d_id == dapp_id]
    if len(categories) == 0:
        return False
    if len(set(categories).difference(set(category_ids))) > 0:
        return False
    return True

TEST_CATEGORY_DATA_AS_MAP = {
    cat_id: name for cat_id, name in TEST_CATEGORY_DATA
}
# NOTE: this is a hacky way to ensure tests pass on linux and mac
# the issue being that postgres by default is installed with
# different LC_COLLATE settings between linux (using "en_GB.UTF-8") and
# mac (using "C", at least when installing via brew) and this effects
# the sort order of "ORDER BY {VARCHAR}" operations.
if 'LC_COLLATE' in os.environ:
    LC_COLLATE = os.environ["LC_COLLATE"]
else:
    print("$LC_COLLATE is not set, guessing LC_COLLATE based on system platform. If tests fail unexpectedly due to ordering, check this value")
    if sys.platform == 'linux':
        LC_COLLATE = 'en_GB.UTF-8'
    elif sys.platform == 'darwin':
        LC_COLLATE = 'C'
    else:
        print("Unsupported system.platform '{}'".format(sys.platform))
        sys.exit(1)

if LC_COLLATE == 'C':
    ALL_DAPPS_SORTED = sorted(TEST_DAPP_DATA, key=lambda e: e[1])
    GAMES_AND_COLLECTIBLES_SORTED = [y for y in sorted(TEST_DAPP_DATA, key=lambda e: e[1]) if y[0] in [x[0] for x in TEST_DAPP_CATEGORY_DATA if x[1] == 1]]
    GAMES_AND_COLLECTIBLES_SORTED_BY_RANK = [y for y in sorted(TEST_DAPP_DATA, key=lambda e: (-e[7], e[1])) if y[0] in [x[0] for x in TEST_DAPP_CATEGORY_DATA if x[1] == 1]]
else:
    ALL_DAPPS_SORTED = sorted(TEST_DAPP_DATA, key=lambda e: e[1].replace(' ', '').lower())
    GAMES_AND_COLLECTIBLES_SORTED = [y for y in sorted(TEST_DAPP_DATA, key=lambda e: e[1].replace(' ', '').lower()) if y[0] in [x[0] for x in TEST_DAPP_CATEGORY_DATA if x[1] == 1]]
    GAMES_AND_COLLECTIBLES_SORTED_BY_RANK = [y for y in sorted(TEST_DAPP_DATA, key=lambda e: (-e[7], e[1].replace(' ', '').lower())) if y[0] in [x[0] for x in TEST_DAPP_CATEGORY_DATA if x[1] == 1]]

GAMES_AND_COLLECTIBLES_CATEGORY = 1
TEST_QUERY = 'ether'
DAPPS_WITH_QUERY_IN_NAME_SORTED = [x for x in ALL_DAPPS_SORTED if TEST_QUERY in x[1].lower()]
GAMES_AND_COLLECTIBLES_WITH_QUERY_IN_NAME_SORTED = [x for x in GAMES_AND_COLLECTIBLES_SORTED if TEST_QUERY in x[1].lower()]

class DappsTestBase(AsyncHandlerTest):

    def get_urls(self):
        return urls

    def get_url(self, path):
        path = "/v1{}".format(path)
        return super().get_url(path)

    async def create_test_data(self, with_filters=False):
        async with self.pool.acquire() as con:
            await con.executemany(
                "INSERT INTO dapps (dapp_id, name, url, description, icon, cover, created, updated, rank) "
                "VALUES ($1, $2, $3, $4, $5, $5, $6, $7, $8)",
                TEST_DAPP_DATA)
            await con.executemany(
                "INSERT INTO categories (category_id, name) "
                "VALUES ($1, $2)",
                TEST_CATEGORY_DATA)
            await con.executemany(
                "INSERT INTO dapp_categories (dapp_id, category_id) "
                "VALUES ($1, $2)",
                TEST_DAPP_CATEGORY_DATA)
            if with_filters:
                await con.executemany(
                    "UPDATE dapps SET hidden_on = 'ios' WHERE dapp_id = $1",
                    [(e,) for e in TEST_IOS_HIDDEN_DAPPS])
                await con.executemany(
                    "UPDATE dapps SET hidden_on = 'android' WHERE dapp_id = $1",
                    [(e,) for e in TEST_ANDROID_HIDDEN_DAPPS])
                await con.executemany(
                    "UPDATE dapps SET hidden_on = 'all' WHERE dapp_id = $1",
                    [(e,) for e in TEST_ALL_HIDDEN_DAPPS])
                await con.executemany(
                    "UPDATE categories SET hidden_on = 'ios' WHERE category_id = $1",
                    [(e,) for e in TEST_IOS_HIDDEN_CATEGORIES])
                await con.executemany(
                    "UPDATE categories SET hidden_on = 'android' WHERE category_id = $1",
                    [(e,) for e in TEST_ANDROID_HIDDEN_CATEGORIES])
                await con.executemany(
                    "UPDATE categories SET hidden_on = 'all' WHERE category_id = $1",
                    [(e,) for e in TEST_ALL_HIDDEN_CATEGORIES])

def exist_valid_dapp_for_category(category, hidden_dapps):
    dapps = [ dapp for dapp in TEST_DAPP_DATA if dapp[0] not in hidden_dapps and dapp[0] not in TEST_ALL_HIDDEN_DAPPS and (dapp[0], category) in TEST_DAPP_CATEGORY_DATA]
    return len(dapps) > 0

class FrontpageHandlerTest(DappsTestBase):

    @gen_test
    @requires_database
    async def test_frontpage(self):

        await self.create_test_data(with_filters=True)

        for test_type, test_user_agent, hidden_categories, hidden_dapps in [
                ('no filtering', TEST_UNKNOWN_USER_AGENT, [], []),
                ('ios filtering', TEST_IOS_USER_AGENT, TEST_IOS_HIDDEN_CATEGORIES, TEST_IOS_HIDDEN_DAPPS),
                ('android filtering', TEST_ANDROID_USER_AGENT, TEST_ANDROID_HIDDEN_CATEGORIES, TEST_ANDROID_HIDDEN_DAPPS)
        ]:

            resp = await self.fetch("/dapps/frontpage", headers={'User-Agent': test_user_agent})
            self.assertResponseCodeEqual(resp, 200)
            body = json_decode(resp.body)

            self.assertIn("sections", body)
            self.assertIn("categories", body)

            test_category_data = [
                cat for cat in TEST_CATEGORY_DATA if cat[0] not in hidden_categories and cat[0] not in TEST_ALL_HIDDEN_CATEGORIES and exist_valid_dapp_for_category(cat[0], hidden_dapps)
            ]

            # make sure all the categories are listed
            self.assertEqual(len(body['categories']), len(test_category_data))
            # make sure there is a section per category
            self.assertEqual(len(body['sections']), len(test_category_data))

            # make sure the sections are listed in order of category_id
            for section, category in zip(body['sections'], test_category_data):
                self.assertEqual(section['category_id'], category[0])

            self.assertEqual(body['sections'][0]['category_id'], 1)
            self.assertIn("dapps", body['sections'][0])
            self.assertEqual(len(body['sections'][0]['dapps']), DAPPS_PER_CATEGORY)

            for result, expected in zip(body['sections'][0]['dapps'], GAMES_AND_COLLECTIBLES_SORTED_BY_RANK[:DAPPS_PER_CATEGORY]):
                self.assertEqual(len(result), 7)
                self.assertEqual(result['dapp_id'], expected[0])
                self.assertEqual(result['name'], expected[1])
                self.assertEqual(result['url'], expected[2])
                self.assertEqual(result['description'], expected[3])
                self.assertEqual(result['icon'], expected[4])
                self.assertEqual(result['cover'], expected[4])
                expected_categories = [x[1] for x in TEST_DAPP_CATEGORY_DATA if x[0] == result['dapp_id']]
                self.assertEqual(len(result['categories']), len(expected_categories))
                for cat in result['categories']:
                    self.assertIn(cat, expected_categories)
            for cat in body['categories']:
                self.assertEqual(body['categories'][cat], TEST_CATEGORY_DATA_AS_MAP[int(cat)])

class DappSearchHandlerTest(DappsTestBase):

    def assertDappSearchResults(self, body, query, expected_offset, expected_limit, expected_total, expected_results, expected_category, hidden_categories):

        self.assertIn("results", body)
        self.assertIn("dapps", body["results"])
        self.assertIn("categories", body["results"])
        dapps = body["results"]["dapps"]

        self.assertEqual(len(dapps), len(expected_results))
        self.assertIn("limit", body)
        self.assertEqual(body['limit'], expected_limit)
        self.assertIn("offset", body)
        self.assertEqual(body['offset'], expected_offset)
        self.assertIn("total", body)
        self.assertEqual(body['total'], expected_total)
        self.assertIn("query", body)
        self.assertEqual(body['query'], query)
        self.assertIn("category", body)
        self.assertEqual(body['category'], expected_category)

        used_categories = set()
        for index, (result, expected) in enumerate(zip(dapps, expected_results)):
            self.assertEqual(len(result), 7)
            self.assertEqual(result['name'], expected[1], "mismatched results at index: {}".format(index))
            self.assertEqual(result['dapp_id'], expected[0])
            self.assertEqual(result['url'], expected[2])
            self.assertEqual(result['description'], expected[3])
            self.assertEqual(result['icon'], expected[4])
            self.assertEqual(result['cover'], expected[4])
            expected_categories = [x[1] for x in TEST_DAPP_CATEGORY_DATA if x[0] == result['dapp_id'] and x[1] not in hidden_categories]
            self.assertEqual(len(result['categories']), len(expected_categories))
            for cat in result['categories']:
                self.assertIn(cat, expected_categories)
                used_categories.add(cat)
        # make sure all the categories used by dapps in this query (and no more) are present in the body
        self.assertEqual(len(used_categories), len(body['results']['categories']))
        for cat in used_categories:
            self.assertIn(str(cat), body['results']['categories'])
            self.assertEqual(body['results']['categories'][str(cat)], TEST_CATEGORY_DATA_AS_MAP[cat])

    @gen_test
    @requires_database
    async def test_all_dapps(self):

        await self.create_test_data()

        resp = await self.fetch("/dapps")
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)

        self.assertLessEqual(DEFAULT_DAPP_SEARCH_LIMIT * 2, len(TEST_DAPP_DATA),
                             "Test DAPP data isn't a large as the default search limit, add some more test data!")

        self.assertDappSearchResults(body, "", 0, DEFAULT_DAPP_SEARCH_LIMIT, len(TEST_DAPP_DATA),
                                     ALL_DAPPS_SORTED[:DEFAULT_DAPP_SEARCH_LIMIT], None, [])

        # make sure paging works as expected
        resp = await self.fetch("/dapps?offset={}".format(body['limit']))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)

        self.assertDappSearchResults(body, "", DEFAULT_DAPP_SEARCH_LIMIT, DEFAULT_DAPP_SEARCH_LIMIT,
                                     len(TEST_DAPP_DATA),
                                     ALL_DAPPS_SORTED[DEFAULT_DAPP_SEARCH_LIMIT:DEFAULT_DAPP_SEARCH_LIMIT * 2],
                                     None, [])

    @gen_test
    @requires_database
    async def test_all_dapps_with_filtering(self):

        await self.create_test_data(with_filters=True)

        for test_type, test_user_agent, hidden_categories, hidden_dapps in [
                ('no filtering', TEST_UNKNOWN_USER_AGENT, [], []),
                ('ios filtering', TEST_IOS_USER_AGENT, TEST_IOS_HIDDEN_CATEGORIES, TEST_IOS_HIDDEN_DAPPS),
                ('android filtering', TEST_ANDROID_USER_AGENT, TEST_ANDROID_HIDDEN_CATEGORIES, TEST_ANDROID_HIDDEN_DAPPS)
        ]:
            hidden_categories.extend(TEST_ALL_HIDDEN_CATEGORIES)
            hidden_dapps.extend(TEST_ALL_HIDDEN_DAPPS)
            test_dapp_data = [dapp for dapp in TEST_DAPP_DATA if
                              dapp[0] not in hidden_dapps and
                              not dapp_only_in_categories(dapp[0], hidden_categories)]
            all_dapps_sorted = [dapp for dapp in ALL_DAPPS_SORTED if
                                dapp[0] not in hidden_dapps and
                                not dapp_only_in_categories(dapp[0], hidden_categories)]

            resp = await self.fetch("/dapps", headers={"User-Agent": test_user_agent})
            self.assertResponseCodeEqual(resp, 200)
            body = json_decode(resp.body)
            self.assertLessEqual(DEFAULT_DAPP_SEARCH_LIMIT * 2, len(test_dapp_data),
                                 "Test DAPP data isn't a large as the default search limit, add some more test data!")

            self.assertDappSearchResults(body, "", 0, DEFAULT_DAPP_SEARCH_LIMIT, len(test_dapp_data),
                                         all_dapps_sorted[:DEFAULT_DAPP_SEARCH_LIMIT], None, hidden_categories)

            # make sure paging works as expected
            resp = await self.fetch("/dapps?offset={}".format(body['limit']), headers={"User-Agent": test_user_agent})
            self.assertResponseCodeEqual(resp, 200)
            body = json_decode(resp.body)

            self.assertDappSearchResults(body, "", DEFAULT_DAPP_SEARCH_LIMIT, DEFAULT_DAPP_SEARCH_LIMIT,
                                         len(test_dapp_data),
                                         all_dapps_sorted[DEFAULT_DAPP_SEARCH_LIMIT:DEFAULT_DAPP_SEARCH_LIMIT * 2],
                                         None, hidden_categories)

    @gen_test
    @requires_database
    async def test_bad_paging_query(self):

        await self.create_test_data()

        # make sure values that aren't numbers return an error
        for query in ["offset=foo", "limit=bar", "offset=5&limit=foo", "limit=10&offset=bar", "limit=None", "offset=null", "category=foo", "category=null"]:
            resp = await self.fetch("/dapps?{}".format(query))
            self.assertResponseCodeEqual(resp, 400)

    @gen_test
    @requires_database
    async def test_max_query_limit(self):

        await self.create_test_data()

        resp = await self.fetch("/dapps?limit={}".format(MAX_DAPP_SEARCH_LIMIT * 2))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)

        self.assertIn("limit", body)
        # make sure the limit is forced to be the max
        self.assertEqual(body['limit'], MAX_DAPP_SEARCH_LIMIT)

    @gen_test
    @requires_database
    async def test_dapp_search_with_query(self):

        await self.create_test_data()

        limit = len(DAPPS_WITH_QUERY_IN_NAME_SORTED) // 2
        # must be enough results to handle doing 3 pages of queries
        self.assertLess(limit * 2, len(DAPPS_WITH_QUERY_IN_NAME_SORTED),
                        "Not enough available results from the Test DAPP data matching the test query, add some more test data!")

        resp = await self.fetch("/dapps?limit={}&query={}".format(limit, TEST_QUERY))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)

        self.assertDappSearchResults(body, TEST_QUERY, 0, limit, len(DAPPS_WITH_QUERY_IN_NAME_SORTED),
                                     DAPPS_WITH_QUERY_IN_NAME_SORTED[:limit], None, [])

        resp = await self.fetch("/dapps?offset={}&limit={}&query={}".format(limit, limit, TEST_QUERY))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)

        self.assertDappSearchResults(body, TEST_QUERY, limit, limit, len(DAPPS_WITH_QUERY_IN_NAME_SORTED),
                                     DAPPS_WITH_QUERY_IN_NAME_SORTED[limit:limit * 2], None, [])

        resp = await self.fetch("/dapps?offset={}&limit={}&query={}".format(limit * 2, limit, TEST_QUERY))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)

        self.assertDappSearchResults(body, TEST_QUERY, limit * 2, limit, len(DAPPS_WITH_QUERY_IN_NAME_SORTED),
                                     DAPPS_WITH_QUERY_IN_NAME_SORTED[limit * 2:], None, [])

        resp = await self.fetch("/dapps?query={}".format('ZZZZZZZZZZ'))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)

        self.assertDappSearchResults(body, 'ZZZZZZZZZZ', 0, 10, 1,
                                     [TEST_DAPP_DATA[-5]], None, [])

    @gen_test
    @requires_database
    async def test_dapp_category_search(self):

        await self.create_test_data()

        limit = len(GAMES_AND_COLLECTIBLES_SORTED) // 2
        if len(GAMES_AND_COLLECTIBLES_SORTED) % 2 == 0:
            limit -= 1

        # must be enough results to handle doing 3 pages of queries
        self.assertLess(limit * 2, len(GAMES_AND_COLLECTIBLES_SORTED),
                        "Not enough available results from the Test DAPP data matching the test query, add some more test data!")

        resp = await self.fetch("/dapps?limit={}&category={}".format(limit, GAMES_AND_COLLECTIBLES_CATEGORY))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)

        self.assertDappSearchResults(body, "", 0, limit, len(GAMES_AND_COLLECTIBLES_SORTED),
                                     GAMES_AND_COLLECTIBLES_SORTED[:limit], GAMES_AND_COLLECTIBLES_CATEGORY, [])

        resp = await self.fetch("/dapps?offset={}&limit={}&category={}".format(limit, limit, GAMES_AND_COLLECTIBLES_CATEGORY))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)

        self.assertDappSearchResults(body, "", limit, limit, len(GAMES_AND_COLLECTIBLES_SORTED),
                                     GAMES_AND_COLLECTIBLES_SORTED[limit:limit * 2], GAMES_AND_COLLECTIBLES_CATEGORY, [])

        resp = await self.fetch("/dapps?offset={}&limit={}&category={}".format(limit * 2, limit, GAMES_AND_COLLECTIBLES_CATEGORY))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)

        self.assertDappSearchResults(body, "", limit * 2, limit, len(GAMES_AND_COLLECTIBLES_SORTED),
                                     GAMES_AND_COLLECTIBLES_SORTED[limit * 2:], GAMES_AND_COLLECTIBLES_CATEGORY, [])

    @gen_test
    @requires_database
    async def test_dapp_category_search_with_query(self):

        await self.create_test_data()

        limit = len(GAMES_AND_COLLECTIBLES_WITH_QUERY_IN_NAME_SORTED) // 2
        # must be enough results to handle doing 3 pages of queries
        self.assertLess(limit * 2, len(GAMES_AND_COLLECTIBLES_WITH_QUERY_IN_NAME_SORTED),
                        "Not enough available results from the Test DAPP data matching the test query, add some more test data!")

        resp = await self.fetch("/dapps?limit={}&category={}&query={}".format(limit, GAMES_AND_COLLECTIBLES_CATEGORY, TEST_QUERY))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)

        self.assertDappSearchResults(body, TEST_QUERY, 0, limit, len(GAMES_AND_COLLECTIBLES_WITH_QUERY_IN_NAME_SORTED),
                                     GAMES_AND_COLLECTIBLES_WITH_QUERY_IN_NAME_SORTED[:limit], GAMES_AND_COLLECTIBLES_CATEGORY, [])

        resp = await self.fetch("/dapps?offset={}&limit={}&category={}&query={}".format(limit, limit, GAMES_AND_COLLECTIBLES_CATEGORY, TEST_QUERY))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)

        self.assertDappSearchResults(body, TEST_QUERY, limit, limit, len(GAMES_AND_COLLECTIBLES_WITH_QUERY_IN_NAME_SORTED),
                                     GAMES_AND_COLLECTIBLES_WITH_QUERY_IN_NAME_SORTED[limit:limit * 2], GAMES_AND_COLLECTIBLES_CATEGORY, [])

        resp = await self.fetch("/dapps?offset={}&limit={}&category={}&query={}".format(limit * 2, limit, GAMES_AND_COLLECTIBLES_CATEGORY, TEST_QUERY))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)

        self.assertDappSearchResults(body, TEST_QUERY, limit * 2, limit, len(GAMES_AND_COLLECTIBLES_WITH_QUERY_IN_NAME_SORTED),
                                     GAMES_AND_COLLECTIBLES_WITH_QUERY_IN_NAME_SORTED[limit * 2:], GAMES_AND_COLLECTIBLES_CATEGORY, [])

    @gen_test
    @requires_database
    async def test_filtered_dapp_category_search(self):

        await self.create_test_data(with_filters=True)

        for test_type, test_user_agent, hidden_categories, hidden_dapps in [
                ('no filtering', TEST_UNKNOWN_USER_AGENT, [], []),
                ('ios filtering', TEST_IOS_USER_AGENT, TEST_IOS_HIDDEN_CATEGORIES, TEST_IOS_HIDDEN_DAPPS),
                ('android filtering', TEST_ANDROID_USER_AGENT, TEST_ANDROID_HIDDEN_CATEGORIES, TEST_ANDROID_HIDDEN_DAPPS)
        ]:

            hidden_categories.extend(TEST_ALL_HIDDEN_DAPPS)
            limit = len(GAMES_AND_COLLECTIBLES_SORTED) // 2
            if len(GAMES_AND_COLLECTIBLES_SORTED) % 2 == 0:
                limit -= 1

            # must be enough results to handle doing 3 pages of queries
            self.assertLess(limit * 2, len(GAMES_AND_COLLECTIBLES_SORTED),
                            "Not enough available results from the Test DAPP data matching the test query, add some more test data!")

            resp = await self.fetch("/dapps?limit={}&category={}".format(limit, GAMES_AND_COLLECTIBLES_CATEGORY), headers={"User-Agent": test_user_agent})
            self.assertResponseCodeEqual(resp, 200)
            body = json_decode(resp.body)

            self.assertDappSearchResults(body, "", 0, limit, len(GAMES_AND_COLLECTIBLES_SORTED),
                                         GAMES_AND_COLLECTIBLES_SORTED[:limit], GAMES_AND_COLLECTIBLES_CATEGORY, hidden_categories)

class DappHandlerTest(DappsTestBase):
    @gen_test
    @requires_database
    async def test_get_single_dapp(self):

        await self.create_test_data()

        expected = TEST_DAPP_DATA[18]
        self.assertEqual(expected[0], 1713827119085126677, "expected Rare Bits as test data, fix this")

        resp = await self.fetch("/dapp/{}".format(expected[0]))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)
        self.assertIn("dapp", body)
        dapp = body["dapp"]

        self.assertEqual(len(dapp), 7)
        self.assertEqual(dapp['dapp_id'], expected[0])
        self.assertEqual(dapp['name'], expected[1])
        self.assertEqual(dapp['url'], expected[2])
        self.assertEqual(dapp['description'], expected[3])
        self.assertEqual(dapp['icon'], expected[4])
        self.assertEqual(dapp['cover'], expected[4])
        expected_categories = [x[1] for x in TEST_DAPP_CATEGORY_DATA if x[0] == dapp['dapp_id']]
        self.assertEqual(len(dapp['categories']), len(expected_categories))
        self.assertEqual(len(body['categories']), len(expected_categories))
        for cat in dapp['categories']:
            self.assertIn(cat, expected_categories)
        for cat in expected_categories:
            self.assertIn(str(cat), body['categories'])
            self.assertEqual(body['categories'][str(cat)], TEST_CATEGORY_DATA_AS_MAP[cat])

    @gen_test
    @requires_database
    async def test_dapp_404(self):
        resp = await self.fetch("/dapp/123")
        self.assertResponseCodeEqual(resp, 404)
