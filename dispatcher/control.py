import logging
import uuid
import json
import time

logger = logging.getLogger('awx.main.dispatch')


class Control(object):
    services = ('dispatcher', 'callback_receiver')
    result = None

    def __init__(self, queue, config=None):
        self.queuename = queue
        self.config = config

    def running(self, *args, **kwargs):
        return self.control_with_reply('running', *args, **kwargs)

    def cancel(self, task_ids, with_reply=True):
        if with_reply:
            return self.control_with_reply('cancel', extra_data={'task_ids': task_ids})
        else:
            self.control({'control': 'cancel', 'task_ids': task_ids, 'reply_to': None}, extra_data={'task_ids': task_ids})

    def schedule(self, *args, **kwargs):
        return self.control_with_reply('schedule', *args, **kwargs)

    @classmethod
    def generate_reply_queue_name(cls):
        return f"reply_to_{str(uuid.uuid4()).replace('-','_')}"

    def get_connection(self):
        from dispatcher.brokers.pg_notify import get_connection, get_django_connection

        if self.config:
            connection = get_connection(self.config)
        else:
            connection = get_django_connection()

        return connection

    # TODO: implement in broker
    def notify(self, connection, data):
        payload = json.dumps(data)
        connection.execute('SELECT pg_notify(%s, %s);', (self.queuename, payload))

    def control_with_reply(self, command, timeout=1, data=None):
        logger.warning('checking {} for {}'.format(command, self.queuename))
        reply_queue = Control.generate_reply_queue_name()
        self.result = None

        connection = self.get_connection()

        # if not connection.get_autocommit():
        #     raise RuntimeError('Control-with-reply messages can only be done in autocommit mode')

        replies = []

        def save_reply(n):
            replies.append((n.channel, n.payload))

        # TODO: implement in broker... may not support all brokers
        connection.execute(f"LISTEN {reply_queue}")
        connection.add_notify_handler(save_reply)

        send_data = {'control': command, 'reply_to': reply_queue}
        if data:
            send_data.update(data)
        self.notify(connection, send_data)

        time.sleep(timeout)

        connection.execute("SELECT 1").fetchone()

        parsed_replies = [json.loads(payload) for q, payload in replies]

        return parsed_replies

    def control(self, data):
        connection = self.get_connection()
        self.notify(connection, data)
