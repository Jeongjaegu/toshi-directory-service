import datetime

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
     datetime.datetime(2017, 12, 19, 12, 29, 20, 552493)),
    (1673246774900294659,
     'NameBazaar',
     'https://namebazaar.io/',
     'A peer-to-peer marketplace for the exchange of names registered via the Ethereum Name Service.',
     'https://www.toshi.org/0x0000000000000000000000001738911397800403_a64bad.png',
     datetime.datetime(2017, 12, 19, 12, 29, 41, 692435),
     datetime.datetime(2017, 12, 19, 12, 29, 41, 692435)),
    (1673246900729414660,
     'Cryptokitties',
     'https://www.cryptokitties.co/',
     'CryptoKitties is a game centered around breedable, collectible, and oh-so-adorable creatures we call CryptoKitties! Each cat is one-of-a-kind and 100% owned by you; it cannot be replicated, taken away, or destroyed.',
     'https://www.toshi.org/0x00000000000000000000000017389130e3800404_3ac912.png',
     datetime.datetime(2017, 12, 19, 12, 29, 56, 273100),
     datetime.datetime(2017, 12, 19, 12, 29, 56, 273100)),
    (1674816702887494661,
     'Ethlance',
     'https://ethlance.com/',
     'The future of work is now! Hire or work for Ether cryptocurrency',
     'https://www.toshi.org/0x000000000000000000000000173e24eaef800405_f1b0b7.png',
     datetime.datetime(2017, 12, 21, 16, 28, 51, 564216),
     datetime.datetime(2017, 12, 21, 16, 28, 51, 564216)),
    (1674820117050950662,
     'Leeroy',
     'https://leeroy.io/',
     'Leeroy is a decentralised social network built on Ethereum.',
     'https://www.toshi.org/0x000000000000000000000000173e2805db800406_7ad6b4.png',
     datetime.datetime(2017, 12, 21, 16, 35, 38, 270798),
     datetime.datetime(2017, 12, 21, 16, 35, 38, 270798)),
    (1674821140461126663,
     'CryptoPunks',
     'https://www.larvalabs.com/cryptopunks',
     '10,000 unique collectible characters with proof of ownership stored on the Ethereum blockchain.',
     'https://www.toshi.org/0x000000000000000000000000173e28f423800407_c9007c.jpg',
     datetime.datetime(2017, 12, 21, 16, 37, 40, 235798),
     datetime.datetime(2017, 12, 21, 16, 37, 40, 235798)),
    (1695885715289670663,
     'Crypto High Score',
     'https://www.cryptohighscore.co',
     'The first high score leaderboard on the blockchain. Pay your way to the top of the immutable, irrefutable global rankings.',
     'https://www.toshi.org/0x0000000000000000000000001788ff12a7800407_ca5ba0.png',
     datetime.datetime(2018, 1, 19, 18, 9, 13, 815644),
     datetime.datetime(2018, 1, 19, 18, 9, 13, 815644)),
    (1699282069730886664,
     'ERC dEX',
     'https://app.ercdex.com/',
     'Trustless\xa0trading has arrived on Ethereum. No need to deposit your tokens or create an account - trade ERC20 tokens directly from your wallet.',
     'https://www.toshi.org/0x0000000000000000000000001795100a0b800408_543c99.png',
     datetime.datetime(2018, 1, 24, 10, 37, 10, 994011),
     datetime.datetime(2018, 1, 24, 10, 37, 10, 994011)),
    (1704426417141318665,
     'ChainMonsters',
     'https://chainmonsters.io/',
     'ChainMonsters is a 100% blockchain based monster collectible game. Every action you take, every ChainMonster you catch will be reflected in the game and on blockchain itself.',
     'https://www.toshi.org/0x00000000000000000000000017a756cbc3800409_393169.png',
     datetime.datetime(2018, 1, 31, 12, 58, 4, 697837),
     datetime.datetime(2018, 1, 31, 12, 58, 4, 697837)),
    (1708300997736006666,
     'Etheremon',
     'https://www.etheremon.com/',
     'A decentralized application built on the Ethereum network to simulate a world of monsters where you can capture, evolve a monster to defeat others.',
     'https://www.toshi.org/0x00000000000000000000000017b51ab4db80040a_00dfcb.png',
     datetime.datetime(2018, 2, 5, 21, 16, 10, 638845),
     datetime.datetime(2018, 2, 5, 21, 16, 10, 638845)),
    (1708333788804678667,
     'Ethercraft',
     'https://ethercraft.io/',
     'A decentralized RPG running on the Ethereum blockchain.',
     'https://www.toshi.org/0x00000000000000000000000017b538879f80040b_944085.png',
     datetime.datetime(2018, 2, 5, 22, 21, 19, 564400),
     datetime.datetime(2018, 2, 5, 22, 21, 19, 564400)),
    (1708349878154822668,
     'Etherbots',
     'https://etherbots.io/',
     'A decentralized Robot Wars game for the Ethereum blockchain.',
     'https://www.toshi.org/0x00000000000000000000000017b54729b780040c_ecda6e.jpg',
     datetime.datetime(2018, 2, 5, 22, 53, 18, 15577),
     datetime.datetime(2018, 2, 5, 22, 53, 18, 15577)),
    (1708351748814406669,
     'Hong Bao',
     'https://givehongbao.com/',
     'Crowdfunded campaigns powered by the Ethereum Network. Hong Bao lets you raise money for any cause, anywhere in the world.',
     'https://www.toshi.org/0x00000000000000000000000017b548dd4380040d_c823c8.jpg',
     datetime.datetime(2018, 2, 5, 22, 57, 0, 946397),
     datetime.datetime(2018, 2, 5, 22, 57, 0, 946397)),
    (1708352654784070670,
     'World of Ether',
     'https://worldofether.com/',
     'Fully decentralized collectable duelling game on the Ethereum blockchain. Collect, breed, battle.',
     'https://www.toshi.org/0x00000000000000000000000017b549b03380040e_b6b3c8.jpg',
     datetime.datetime(2018, 2, 5, 22, 58, 48, 948025),
     datetime.datetime(2018, 2, 5, 22, 58, 48, 948025)),
    (1710548330985030671,
     'CryptoFighters',
     'https://cryptofighters.io/',
     'CryptoFighters is a game centred around cryptographically unique collectible fighters on the Ethereum blockchain. Collect, battle and level up your fighters to win new fighters!',
     'https://www.toshi.org/0x00000000000000000000000017bd16a4e780040f_0bb1e3.png',
     datetime.datetime(2018, 2, 8, 23, 41, 13, 585866),
     datetime.datetime(2018, 2, 8, 23, 41, 13, 585866)),
    (1710550109369926672,
     'Crypto Celebrities',
     'https://www.cryptocelebrities.co/',
     'Collect one-of-a-kind celebrity smart contracts',
     'https://www.toshi.org/0x00000000000000000000000017bd1842f7800410_73effb.png',
     datetime.datetime(2018, 2, 8, 23, 44, 45, 829020),
     datetime.datetime(2018, 2, 8, 23, 44, 45, 829020)),
    (1710555771680326675,
     'Ether Numbers',
     'https://ethernumbers.co/',
     'EtherNumbers is a marketplace for collectible, tokenized representations of your favorite numbers. Each EtherNumber is like a baseball card, except there can only ever be one!',
     'https://www.toshi.org/0x00000000000000000000000017bd1d6953800413_e028d0.jpg',
     datetime.datetime(2018, 2, 8, 23, 56, 0, 959724),
     datetime.datetime(2018, 2, 8, 23, 56, 0, 959724)),
    (1710556308551238676,
     'OpenSea',
     'https://opensea.io/',
     'Peer-to-peer marketplace for rare digital items.',
     'https://www.toshi.org/0x00000000000000000000000017bd1de653800414_308bf8.png',
     datetime.datetime(2018, 2, 8, 23, 57, 4, 888989),
     datetime.datetime(2018, 2, 8, 23, 57, 4, 888989)),
    (1713827119085126677,
     'Rare Bits',
     'https://rarebits.io/',
     'A zero fee marketplace for crypto assets.',
     'https://www.toshi.org/0x00000000000000000000000017c8bcaf3f800415_2ff16a.png',
     datetime.datetime(2018, 2, 13, 12, 15, 35, 882713),
     datetime.datetime(2018, 2, 13, 12, 15, 35, 882713)),
    (1715336347104838676,
     'Crypto Speech',
     'http://cryptospeech.com/',
     'Decentralized, Twitter-style messages stored for eternity',
     'https://www.toshi.org/0x00000000000000000000000017ce1951c7800414_2649b0.png',
     datetime.datetime(2018, 2, 15, 14, 14, 9, 986518),
     datetime.datetime(2018, 2, 15, 14, 14, 9, 986518)),
    (1721937401460294677,
     'Kpopio',
     'https://www.kpop.io/',
     'Kpopio is an online game where you can buy and sell Kpop celebrity cards with other players.',
     'https://www.toshi.org/0x00000000000000000000000017e58cf183800415_3ba268.png',
     datetime.datetime(2018, 2, 24, 16, 49, 16, 855046),
     datetime.datetime(2018, 2, 24, 16, 49, 16, 855046))
]

TEST_CATEGORY_DATA = [
    (1, "Games & Collectibles"),
    (2, "Marketplaces"),
    (3, "Jobs"),
    (4, "Social Media"),
    (5, "Crowdfunding"),
    (6, "Exchanges")
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
    (1699282069730886664, 6),
    (1704426417141318665, 1),
    (1708300997736006666, 1),
    (1708333788804678667, 1),
    (1708349878154822668, 1),
    (1708351748814406669, 5),
    (1708352654784070670, 1),
    (1710548330985030671, 1),
    (1710550109369926672, 1),
    (1710555771680326675, 1),
    (1710556308551238676, 1),
    (1710556308551238676, 2),
    (1713827119085126677, 1),
    (1713827119085126677, 2),
    (1715336347104838676, 4),
    (1721937401460294677, 1)
]

ALL_DAPPS_SORTED = sorted(TEST_DAPP_DATA, key=lambda e: e[1])
GAMES_AND_COLLECTIBLES_SORTED = [y for y in sorted(TEST_DAPP_DATA, key=lambda e: e[1]) if y[0] in [x[0] for x in TEST_DAPP_CATEGORY_DATA if x[1] == 1]]
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

    async def create_test_data(self):
        async with self.pool.acquire() as con:
            await con.executemany(
                "INSERT INTO dapps (dapp_id, name, url, description, icon, cover, created, updated) "
                "VALUES ($1, $2, $3, $4, $5, $5, $6, $7)",
                TEST_DAPP_DATA)
            await con.executemany(
                "INSERT INTO categories (category_id, name) "
                "VALUES ($1, $2)",
                TEST_CATEGORY_DATA)
            await con.executemany(
                "INSERT INTO dapp_categories (dapp_id, category_id) "
                "VALUES ($1, $2)",
                TEST_DAPP_CATEGORY_DATA)

class FrontpageHandlerTest(DappsTestBase):

    @gen_test
    @requires_database
    async def test_frontpage(self):

        await self.create_test_data()

        resp = await self.fetch("/dapps/frontpage")
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)

        self.assertIn("sections", body)
        self.assertIn("categories", body)

        # make sure all the categories are listed
        self.assertEqual(len(body['categories']), len(TEST_CATEGORY_DATA))
        # make sure there is a section per category
        self.assertEqual(len(body['sections']), len(TEST_CATEGORY_DATA))

        # make sure the sections are listed in order of category_id
        for section, category in zip(body['sections'], TEST_CATEGORY_DATA):
            self.assertEqual(section['category_id'], category[0])

        self.assertEqual(body['sections'][0]['category_id'], 1)
        self.assertIn("dapps", body['sections'][0])
        self.assertEqual(len(body['sections'][0]['dapps']), DAPPS_PER_CATEGORY)

        for result, expected in zip(body['sections'][0]['dapps'], GAMES_AND_COLLECTIBLES_SORTED[:DAPPS_PER_CATEGORY]):
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

class DappSearchHandlerTest(DappsTestBase):

    def assertDappSearchResults(self, body, query, expected_offset, expected_limit, expected_total, expected_results, expected_category):

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
        for result, expected in zip(dapps, expected_results):
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
                used_categories.add(cat)
        # make sure all the categories used by dapps in this query (and no more) are present in the body
        self.assertEqual(len(used_categories), len(body['results']['categories']))
        for category in used_categories:
            self.assertIn(category, body['results']['categories'])

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
                                     ALL_DAPPS_SORTED[:DEFAULT_DAPP_SEARCH_LIMIT], None)

        # make sure paging works as expected
        resp = await self.fetch("/dapps?offset={}".format(body['limit']))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)

        self.assertDappSearchResults(body, "", DEFAULT_DAPP_SEARCH_LIMIT, DEFAULT_DAPP_SEARCH_LIMIT,
                                     len(TEST_DAPP_DATA),
                                     ALL_DAPPS_SORTED[DEFAULT_DAPP_SEARCH_LIMIT:DEFAULT_DAPP_SEARCH_LIMIT * 2],
                                     None)

    @gen_test
    @requires_database
    async def test_bad_paging_query(self):

        await self.create_test_data()

        # make sure values that aren't numbers return an error
        for query in ["offset=foo", "limit=bar", "offset=5&limit=foo", "limit=10&offset=bar", "limit=None", "offset=null"]:
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
                                     DAPPS_WITH_QUERY_IN_NAME_SORTED[:limit], None)

        resp = await self.fetch("/dapps?offset={}&limit={}&query={}".format(limit, limit, TEST_QUERY))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)

        self.assertDappSearchResults(body, TEST_QUERY, limit, limit, len(DAPPS_WITH_QUERY_IN_NAME_SORTED),
                                     DAPPS_WITH_QUERY_IN_NAME_SORTED[limit:limit * 2], None)

        resp = await self.fetch("/dapps?offset={}&limit={}&query={}".format(limit * 2, limit, TEST_QUERY))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)

        self.assertDappSearchResults(body, TEST_QUERY, limit * 2, limit, len(DAPPS_WITH_QUERY_IN_NAME_SORTED),
                                     DAPPS_WITH_QUERY_IN_NAME_SORTED[limit * 2:], None)

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
                                     GAMES_AND_COLLECTIBLES_SORTED[:limit], GAMES_AND_COLLECTIBLES_CATEGORY)

        resp = await self.fetch("/dapps?offset={}&limit={}&category={}".format(limit, limit, GAMES_AND_COLLECTIBLES_CATEGORY))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)

        self.assertDappSearchResults(body, "", limit, limit, len(GAMES_AND_COLLECTIBLES_SORTED),
                                     GAMES_AND_COLLECTIBLES_SORTED[limit:limit * 2], GAMES_AND_COLLECTIBLES_CATEGORY)

        resp = await self.fetch("/dapps?offset={}&limit={}&category={}".format(limit * 2, limit, GAMES_AND_COLLECTIBLES_CATEGORY))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)

        self.assertDappSearchResults(body, "", limit * 2, limit, len(GAMES_AND_COLLECTIBLES_SORTED),
                                     GAMES_AND_COLLECTIBLES_SORTED[limit * 2:], GAMES_AND_COLLECTIBLES_CATEGORY)

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
                                     GAMES_AND_COLLECTIBLES_WITH_QUERY_IN_NAME_SORTED[:limit], GAMES_AND_COLLECTIBLES_CATEGORY)

        resp = await self.fetch("/dapps?offset={}&limit={}&category={}&query={}".format(limit, limit, GAMES_AND_COLLECTIBLES_CATEGORY, TEST_QUERY))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)

        self.assertDappSearchResults(body, TEST_QUERY, limit, limit, len(GAMES_AND_COLLECTIBLES_WITH_QUERY_IN_NAME_SORTED),
                                     GAMES_AND_COLLECTIBLES_WITH_QUERY_IN_NAME_SORTED[limit:limit * 2], GAMES_AND_COLLECTIBLES_CATEGORY)

        resp = await self.fetch("/dapps?offset={}&limit={}&category={}&query={}".format(limit * 2, limit, GAMES_AND_COLLECTIBLES_CATEGORY, TEST_QUERY))
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)

        self.assertDappSearchResults(body, TEST_QUERY, limit * 2, limit, len(GAMES_AND_COLLECTIBLES_WITH_QUERY_IN_NAME_SORTED),
                                     GAMES_AND_COLLECTIBLES_WITH_QUERY_IN_NAME_SORTED[limit * 2:], GAMES_AND_COLLECTIBLES_CATEGORY)


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
            self.assertIn(cat, body['categories'])
