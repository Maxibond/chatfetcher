import asyncio
import csv
import time

api_id = 1200000
api_hash = 'asddsadsdasd'

limit = 1000  # number of messages per request
n = 10  # number of coroutines
iterations = 511  # how much iterations should be to get all
last_message_id = 510279

dataset_file_name = 'dataset'


async def fetcher(i, channel, idx):
    print(f"loading {i}/{iterations}.. {idx}")
    messages = await client.get_messages(channel, max_id=idx, limit=limit)
    return messages


async def dump(fetchers, w):
    results = await asyncio.gather(*fetchers)
    for result in results:
        for m in result:
            message = m.message
            if type(message) == 'str':
                message = message.replace('†', '')
            w.writerow([m.id, message, m.from_id, m.reply_to_msg_id])


async def fetch():
    dialogs = await client.get_dialogs()
    channel = dialogs[0]
    print(channel.title)

    # Use the dialog somewhere else
    with open(f'${dataset_file_name}.csv', 'w', newline='') as csvfile:
        w = csv.writer(csvfile, delimiter='†',
                       quotechar='`', quoting=csv.QUOTE_MINIMAL)
        w.writerow(['id', 'message', 'from_id', 'reply_to_msg_id'])
        fetchers = []
        idx = last_message_id
        for i in range(1, iterations):
            if i % n == 0:
                await dump(fetchers, w)
                fetchers = []
                continue
            fetchers.append(fetcher(i, channel, idx))
            idx -= limit
            i += 1
        if len(fetchers) > 0:
            await dump(fetchers, w)


if __name__ == '__main__':
    from telethon import TelegramClient, events

    client = TelegramClient('my_new_session', api_id, api_hash)
    client.start()

    print(client.get_me().stringify())

    # client.send_message('me', 'Hello! Talking to you from Telethon')

    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetch())

    @client.on(events.NewMessage(pattern='(?i)hi|hello'))
    async def handler(event):
        print(event)
        await event.respond('Hey!')

    client.run_until_disconnected()

