import json
from typing import List

import telethon
from telethon import events, hints


class Forwarder:
    def __init__(
            self,
            source: hints.Entity,
            destination: hints.Entity,
            latest_id: int
    ):
        self.source = source
        self.destination = destination
        self.latest_id = latest_id


def connect():
    with open("config.json", "r") as f:
        config = json.load(f)
    client = telethon.TelegramClient('auto_forwarder', config["api_id"], config["api_hash"])
    client.start()
    return client


async def read_forwarders(client):
    forwarders = []
    with open("forwarders.json", "r") as f:
        all_data = json.load(f)
        for forwarder_data in all_data["forwards"]:
            source_entity = await client.get_entity(forwarder_data["source"])
            destination_entity = await client.get_entity(forwarder_data["destination"])
            forwarder = Forwarder(
                source_entity,
                destination_entity,
                forwarder_data.get("latest_id", -1)
            )
            forwarders.append(forwarder)
    return forwarders


def save_forwarders(forwarders):
    data = {"forwards": []}
    for forwarder in forwarders:
        forward_data = {
            "source": forwarder.source.id,
            "destination": forwarder.destination.id,
            "latest_id": forwarder.latest_id
        }
        data["forwards"].append(forward_data)
    with open("forwarders.json", "w") as f:
        json.dump(data, f, indent=2)


async def forward_messages(client: telethon.TelegramClient, forwarders: List[Forwarder]):
    for forwarder in forwarders:
        messages_to_forward = []
        async for message in client.iter_messages(forwarder.source):
            if message.id > forwarder.latest_id:
                messages_to_forward.append(message)
        await client.forward_messages(forwarder.destination, messages_to_forward[::-1], forwarder.source)
        if len(messages_to_forward) > 0:
            forwarder.latest_id = messages_to_forward[0].id
    save_forwarders(forwarders)


async def on_new_message(
        client: telethon.TelegramClient,
        forwarders: List[Forwarder],
        message: events.NewMessage.Event
):
    for forwarder in forwarders:
        if message.chat_id == forwarder.source.id:
            await client.forward_messages(forwarder.destination, [message], forwarder.source)
            forwarder.latest_id = message.id
    save_forwarders(forwarders)


def sync(client, coro):
    return client.loop.run_until_complete(coro)


if __name__ == "__main__":
    print("Connecting to telegram")
    client = connect()
    print("Reading forwarders")
    forwarders = sync(client, read_forwarders(client))
    print("Catching up forwarders")
    sync(client, forward_messages(client, forwarders))
    print("Tracking further updates")
    client.add_event_handler(lambda message: on_new_message(client, forwarders, message), events.NewMessage())
    client.run_until_disconnected()
