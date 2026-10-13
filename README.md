# kafka-connect-discord

Kafka Connect connectors for Discord.

- **Source connector** (`com.example.DiscordSourceConnector`): reads messages from a Discord channel and writes them to a Kafka topic. Can optionally read the channel's existing history first.
- **Sink connector**: not implemented yet.

## Discord bot setup

The source connector reads through a Discord bot.

1. Create an application at <https://discord.com/developers/applications> and add a **Bot** to it.
2. Under **Bot**, turn on **Message Content Intent**. Without it, Discord returns empty `content` for most messages.
3. Copy the bot token.
4. Invite the bot to your server. Under **OAuth2 → URL Generator**, select the `bot` scope and the **View Channels** and **Read Message History** permissions, then open the generated URL.
5. Get the channel ID. In Discord, enable **Settings → Advanced → Developer Mode**, then right-click the channel and choose **Copy Channel ID**.

## Configuration

| Property | Required | Default | Description |
|---|---|---|---|
| `discord.token` | yes | | Bot token. Use `${env:DISCORD_TOKEN}` so the token isn't stored in the connector config. |
| `discord.channel.id` | yes | | Channel to read from. |
| `topic` | yes | | Kafka topic to write to. |
| `discord.read.history` | no | `false` | On first start, read all existing messages in the channel before switching to new ones. Once an offset has been committed, the connector always resumes from it. |

The connector stores the last message ID it wrote as its offset. After a restart it reads any messages it missed, then continues with new ones.

## Record format

- **Key**: channel ID (string)
- **Value**: struct with `id`, `channel_id`, `guild_id`, `author_id`, `author_name`, `author_bot`, `content`, `created_at`, `edited_at`, `attachment_urls`
- **Timestamp**: when the message was sent

Edits and deletions are not captured; each message is written once, as it was when the connector read it.

## Running locally

Requires Maven, JDK 17+, and Podman (or Docker) with Compose.

```bash
mvn package                                   # builds target/plugin/discord-connector/
echo "DISCORD_TOKEN=<bot token>" > .env       # .env is git-ignored
podman compose up -d

curl localhost:8083/connector-plugins         # DiscordSourceConnector should be listed
```

The `connect-init` service waits for the worker and then creates the `discord-source` connector from `docker/discord-source-connector.json`. Set `discord.channel.id` in that file before starting. To apply edits to it later, run `podman compose up connect-init`, or PUT it by hand:

```bash
curl -X PUT -H "Content-Type: application/json" \
  --data @docker/discord-source-connector.json \
  localhost:8083/connectors/discord-source/config

curl localhost:8083/connectors/discord-source/status
```

Read the topic:

```bash
podman exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic discord-messages --from-beginning
```

After changing the code, rebuild and restart the worker:

```bash
mvn package && podman restart connect
```
