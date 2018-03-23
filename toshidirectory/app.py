import toshi.web
from toshidirectory import handlers

urls = [
    # api
    (r"^/v1/dapps/frontpage/?$", handlers.FrontpageHandler),
    (r"^/v1/dapps/?$", handlers.DappSearchHandler),
    (r"^/v1/dapp/([0-9]+)/?$", handlers.DappHandler),
]

def main():
    app = toshi.web.Application(urls)
    app.start()
