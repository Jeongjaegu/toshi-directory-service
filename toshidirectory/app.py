import toshi.web
from . import handlers

urls = [
    # api
    (r"^/v1/dapps/frontpage/?$", handlers.FrontpageHandler),
    (r"^/v1/dapps/?$", handlers.DappSearchHandler),
    (r"^/v1/dapp/([0-9]+)/?$", handlers.DappHandler),
]

class Application(toshi.web.Application):

    def process_config(self):
        config = super(Application, self).process_config()

        return config

def main():
    app = Application(urls)
    app.start()
