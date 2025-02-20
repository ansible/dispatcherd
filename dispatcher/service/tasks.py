from ..factories import get_publisher_from_settings
from ..publish import task


@task()
def reply_to_control(reply_channel: str, message: str):
    broker = get_publisher_from_settings()
    broker.publish_message(channel=reply_channel, message=message)
