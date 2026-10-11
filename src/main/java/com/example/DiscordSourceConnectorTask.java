package com.example;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.config.DiscordSourceConnectorConfig;

import net.dv8tion.jda.api.JDA;
import net.dv8tion.jda.api.JDABuilder;
import net.dv8tion.jda.api.entities.Message;
import net.dv8tion.jda.api.entities.MessageHistory;
import net.dv8tion.jda.api.entities.channel.middleman.MessageChannel;
import net.dv8tion.jda.api.events.message.MessageReceivedEvent;
import net.dv8tion.jda.api.hooks.ListenerAdapter;
import net.dv8tion.jda.api.requests.GatewayIntent;

/**
 * Reads messages from one Discord channel.
 *
 * <p>The task has two phases:
 * <ol>
 *   <li><b>Catch-up</b>: page through the channel's history with the REST API, starting after the last
 *       committed message ID (or from the beginning when {@code discord.read.history} is enabled and there is
 *       no committed offset yet). One page of up to 100 messages is returned per {@link #poll()}.</li>
 *   <li><b>Live</b>: emit messages received over the gateway connection.</li>
 * </ol>
 * The gateway listener is registered before catch-up starts, so messages sent during catch-up are queued
 * rather than lost. Discord message IDs increase over time, so anything at or below the last emitted ID is
 * a duplicate and is skipped.
 */
public class DiscordSourceConnectorTask extends SourceTask {

    static final String CHANNEL_ID_FIELD = "channel_id";
    static final String MESSAGE_ID_FIELD = "message_id";

    private static final Logger log = LoggerFactory.getLogger(DiscordSourceConnectorTask.class);

    private static final int HISTORY_PAGE_SIZE = 100;
    private static final int MAX_LIVE_BATCH_SIZE = 500;
    private static final long LIVE_POLL_TIMEOUT_MS = 1000;

    private final BlockingQueue<Message> liveMessages = new LinkedBlockingQueue<>();

    private DiscordSourceConnectorConfig config;
    private Map<String, String> sourcePartition;
    private JDA jda;
    private MessageChannel channel;
    private long lastEmittedId;
    private boolean catchingUp;

    @Override
    public String version() {
        return Version.get();
    }

    @Override
    public void start(Map<String, String> props) {
        config = new DiscordSourceConnectorConfig(props);
        sourcePartition = Map.of(CHANNEL_ID_FIELD, config.channelId());

        Map<String, Object> offset = context.offsetStorageReader().offset(sourcePartition);
        if (offset != null && offset.get(MESSAGE_ID_FIELD) != null) {
            lastEmittedId = Long.parseUnsignedLong((String) offset.get(MESSAGE_ID_FIELD));
            catchingUp = true;
            log.info("Resuming channel {} after message {}", config.channelId(), Long.toUnsignedString(lastEmittedId));
        } else if (config.readHistory()) {
            lastEmittedId = 0;
            catchingUp = true;
            log.info("No committed offset for channel {}; reading history from the beginning", config.channelId());
        } else {
            lastEmittedId = 0;
            catchingUp = false;
            log.info("No committed offset for channel {}; reading new messages only", config.channelId());
        }

        long channelId = Long.parseUnsignedLong(config.channelId());
        try {
            jda = JDABuilder.createLight(config.token(), GatewayIntent.GUILD_MESSAGES, GatewayIntent.MESSAGE_CONTENT)
                .addEventListeners(new ListenerAdapter() {
                    @Override
                    public void onMessageReceived(MessageReceivedEvent event) {
                        if (event.getChannel().getIdLong() == channelId) {
                            liveMessages.offer(event.getMessage());
                        }
                    }
                })
                .build();
            jda.awaitReady();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConnectException("Interrupted while connecting to Discord", e);
        } catch (RuntimeException e) {
            throw new ConnectException("Failed to connect to Discord: " + e.getMessage(), e);
        }

        channel = jda.getChannelById(MessageChannel.class, channelId);
        if (channel == null) {
            throw new ConnectException("Channel " + config.channelId()
                + " not found. Check the ID and that the bot is a member of the server and can view the channel.");
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        if (catchingUp) {
            return pollHistory();
        }
        return pollLive();
    }

    private List<SourceRecord> pollHistory() {
        List<Message> page = new ArrayList<>(MessageHistory
            .getHistoryAfter(channel, Long.toUnsignedString(lastEmittedId))
            .limit(HISTORY_PAGE_SIZE)
            .complete()
            .getRetrievedHistory());

        if (page.size() < HISTORY_PAGE_SIZE) {
            catchingUp = false;
            log.info("Caught up on channel {}; switching to live messages", config.channelId());
        }
        return toRecords(page);
    }

    private List<SourceRecord> pollLive() throws InterruptedException {
        Message first = liveMessages.poll(LIVE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        if (first == null) {
            return null;
        }
        List<Message> batch = new ArrayList<>();
        batch.add(first);
        liveMessages.drainTo(batch, MAX_LIVE_BATCH_SIZE - 1);
        return toRecords(batch);
    }

    private List<SourceRecord> toRecords(List<Message> messages) {
        messages.sort(Comparator.comparingLong(Message::getIdLong));

        List<SourceRecord> records = new ArrayList<>(messages.size());
        for (Message message : messages) {
            if (Long.compareUnsigned(message.getIdLong(), lastEmittedId) <= 0) {
                continue;
            }
            lastEmittedId = message.getIdLong();
            records.add(new SourceRecord(
                sourcePartition,
                Map.of(MESSAGE_ID_FIELD, message.getId()),
                config.topic(),
                null,
                DiscordMessageSchema.KEY_SCHEMA,
                message.getChannel().getId(),
                DiscordMessageSchema.VALUE_SCHEMA,
                DiscordMessageSchema.toStruct(message),
                message.getTimeCreated().toInstant().toEpochMilli()));
        }
        return records.isEmpty() ? null : records;
    }

    @Override
    public void stop() {
        if (jda != null) {
            jda.shutdown();
            try {
                if (!jda.awaitShutdown(Duration.ofSeconds(10))) {
                    jda.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                jda.shutdownNow();
            }
        }
    }
}
