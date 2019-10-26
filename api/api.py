from rethinkdb import r
import falcon

r.connect(host="localhost", port=28015, db='omega').repl()


def get_messages(channel, client=None):
    if client:
        cursor = r.table('messages') \
            .filter(r.row["channel"] == channel) \
            .filter(r.row['client'] == client).run()
    else:
        cursor = r.table('messages').filter(r.row["channel"] == channel).run()
    messages = []
    for document in cursor:
        messages.append(document)
    return messages


class TelegramResource:
    def on_get(self, req, resp):
        """Handles GET requests"""
        qs = req.params
        print(qs)
        messages = get_messages('Telegram', qs['client'])
        resp.media = messages


api = falcon.API()
api.add_route('/messages/telegram', TelegramResource())
