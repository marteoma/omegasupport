from rethinkdb import r
import falcon

r.connect(host='localhost', port=28015, db='omega').repl()


def get_messages(channel, **kwargs):
    query = r.table('messages').filter(r.row['channel'] == channel)
    if 'client' in kwargs:
        query = query.filter(r.row['client'] == kwargs['client'])
    if 'sender' in kwargs:
        query = query.filter(r.row['origin'] == kwargs['sender'])
    cursor = query.run()
    messages = []
    for document in cursor:
        messages.append(document)
    return messages


class MessagesResource:
    def on_get(self, req, resp, sender=None, channel='Telegram'):
        """Handles GET requests"""
        qs = req.params
        kwargs = {}
        if sender:
            kwargs['sender'] = sender
        if 'client' in qs:
            kwargs['client'] = qs['client']
        messages = get_messages(channel.capitalize(), **kwargs)
        resp.media = messages


api = falcon.API()
api.add_route('/messages/{channel}/{sender}', MessagesResource())
