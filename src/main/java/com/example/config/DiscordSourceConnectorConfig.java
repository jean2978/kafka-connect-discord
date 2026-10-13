package com.example.config;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class DiscordSourceConnectorConfig extends AbstractConfig {

    public static final String TOKEN_CONFIG = "discord.token";
    public static final String CHANNEL_ID_CONFIG = "discord.channel.id";
    public static final String TOPIC_CONFIG = "topic";
    public static final String READ_HISTORY_CONFIG = "discord.read.history";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(TOKEN_CONFIG, ConfigDef.Type.PASSWORD, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
            "Discord bot token. The bot needs the Message Content intent enabled and View Channel + "
                + "Read Message History permissions in the channel.")
        .define(CHANNEL_ID_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new SnowflakeValidator(),
            ConfigDef.Importance.HIGH, "ID of the Discord channel to read messages from.")
        .define(TOPIC_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(),
            ConfigDef.Importance.HIGH, "Kafka topic to write messages to.")
        .define(READ_HISTORY_CONFIG, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM,
            "On first start, read the channel's existing messages from the beginning before switching to "
                + "new messages. If false, only messages sent after the connector starts are read. Has no "
                + "effect once the connector has committed an offset; it then always resumes from there.");

    public DiscordSourceConnectorConfig(Map<String, String> props) {
        super(CONFIG_DEF, props);
    }

    public String token() {
        return getPassword(TOKEN_CONFIG).value();
    }

    public String channelId() {
        return getString(CHANNEL_ID_CONFIG);
    }

    public String topic() {
        return getString(TOPIC_CONFIG);
    }

    public boolean readHistory() {
        return getBoolean(READ_HISTORY_CONFIG);
    }

    /** Discord IDs ("snowflakes") are unsigned 64-bit integers written as decimal strings. */
    private static class SnowflakeValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(String name, Object value) {
            if (value == null || !((String) value).matches("\\d{1,20}")) {
                throw new ConfigException(name, value, "Must be a numeric Discord ID");
            }
            try {
                Long.parseUnsignedLong((String) value);
            } catch (NumberFormatException e) {
                throw new ConfigException(name, value, "Too large for a Discord ID");
            }
        }

        @Override
        public String toString() {
            return "Numeric Discord ID";
        }
    }
}
