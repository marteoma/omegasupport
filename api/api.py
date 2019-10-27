import json

from rethinkdb import r
from joblib import load
import falcon
import requests

r.connect(host='localhost', port=28015, db='omega').repl()
clf = load('model.joblib')
vector = load('vector.joblib')


def get_messages(channel, **kwargs):
    query = get_query(channel, kwargs)
    cursor = query.run()
    messages = []
    for document in cursor:
        messages.append(document)
    return messages


def delete_messages(channel, **kwargs):
    query = get_query(channel, kwargs)
    cursor = query.delete().run()
    return cursor


def get_query(channel, kwargs):
    query = r.table('messages').filter(r.row['channel'] == channel)
    if 'client' in kwargs:
        query = query.filter(r.row['client'] == kwargs['client'])
    if 'sender' in kwargs:
        query = query.filter(r.row['origin'] == kwargs['sender'])
    if 'pluck' in kwargs:
        query = query.pluck(kwargs['pluck'])
    return query


class ReportResource:
    def on_get(self, req, resp, sender=None, channel='Telegram'):
        """Handles GET requests"""
        qs = req.params
        kwargs = {'pluck': 'msg'}
        if sender:
            kwargs['sender'] = sender
        if 'client' in qs:
            kwargs['client'] = qs['client']
        messages = get_messages(channel.capitalize(), **kwargs)
        report = []
        for msg in messages:
            value = clf.predict(vector.transform(msg['msg'].split('\s')))[0]
            value = 'Positivo' if value == 1 else 'Negativo'
            result = {
                "msg": msg['msg'],
                "feel": value
            }
            if 'filter' in qs:
                if qs['filter'].lower() == value.lower():
                    report.append(result)
            else:
                report.append(result)
        resp.media = report


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

    def on_delete(self, req, resp, channel=None, sender=None):
        qs = req.params
        kwargs = {}
        if sender:
            kwargs['sender'] = sender
        if 'client' in qs:
            kwargs['client'] = qs['client']
        messages = delete_messages(channel.capitalize(), **kwargs)
        resp.media = messages

    def on_post(self, req, resp, channel=None, sender=None):
        if channel.capitalize() == 'Telegram':
            if sender:
                data = req.stream.read().decode('utf-8')
                if data:
                    data = json.loads(data)
                    url = "https://api.telegram.org/bot939978475:AAHbALPfICKc8x_QEpxC7vIwIwHnlCeQI-Q/sendMessage" \
                          "?chat_id={user}&text={msg}".format(user=sender, msg=data['msg'])
                    response = requests.get(url)
                    resp.media = response.json()
            else:
                resp.status = 400
                resp.media = "Must specify an account to send the message"
        else:
            resp.status = 400
            resp.media = "Now it is not possible to send message through this channel"


api = falcon.API()
api.add_route('/messages/{channel}/{sender}', MessagesResource())
api.add_route('/report/{channel}/{sender}', ReportResource())
