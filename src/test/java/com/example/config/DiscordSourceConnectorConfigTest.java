package com.example.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class DiscordSourceConnectorConfigTest {

    /** A config with every required property set. Copy it and change one thing per test. */
    private static Map<String, String> validProps() {
        Map<String, String> props = new HashMap<>();
        props.put(DiscordSourceConnectorConfig.TOKEN_CONFIG, "test-token");
        props.put(DiscordSourceConnectorConfig.CHANNEL_ID_CONFIG, "1026585783924174882");
        props.put(DiscordSourceConnectorConfig.TOPIC_CONFIG, "discord-messages");
        return props;
    }

    // Worked example: arrange (build props), act (create the config), assert (check the getters).
    @Test
    void parsesValidConfig() {
        DiscordSourceConnectorConfig config = new DiscordSourceConnectorConfig(validProps());

        assertEquals("test-token", config.token());
        assertEquals("1026585783924174882", config.channelId());
        assertEquals("discord-messages", config.topic());
    }

    @Test
    void readHistoryDefaultsToFalse() {
        DiscordSourceConnectorConfig config = new DiscordSourceConnectorConfig(validProps());

        assertFalse(config.readHistory());
    }

    @Test
    void readHistoryCanBeEnabled() {
        Map<String, String> props = validProps();
        props.put(DiscordSourceConnectorConfig.READ_HISTORY_CONFIG, "true");
        DiscordSourceConnectorConfig config = new DiscordSourceConnectorConfig(props);
        
        assertTrue(config.readHistory());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        DiscordSourceConnectorConfig.TOKEN_CONFIG,
        DiscordSourceConnectorConfig.CHANNEL_ID_CONFIG,
        DiscordSourceConnectorConfig.TOPIC_CONFIG
    })
    void missingRequiredPropertyIsRejected(String prop) {
        Map<String, String> props = validProps();
        props.remove(prop);

        assertThrows(ConfigException.class, () -> new DiscordSourceConnectorConfig(props));
    }

    @Test
    void emptyTopicIsRejected() {
        Map<String, String> props = validProps();
        props.put(DiscordSourceConnectorConfig.TOPIC_CONFIG, "");

        assertThrows(ConfigException.class, () -> new DiscordSourceConnectorConfig(props));
    }

    @ParameterizedTest
    @ValueSource(strings = {"abc", "", "123abc", "-1", "1.5"})
    void nonNumericChannelIdIsRejected(String channelId) {
        Map<String, String> props = validProps();
        props.put(DiscordSourceConnectorConfig.CHANNEL_ID_CONFIG, channelId);

        assertThrows(ConfigException.class, () -> new DiscordSourceConnectorConfig(props));
    }

    @Test
    void largestUnsignedLongChannelIdIsAccepted() {
        Map<String, String> props = validProps();
        props.put(DiscordSourceConnectorConfig.CHANNEL_ID_CONFIG, "18446744073709551615");

        DiscordSourceConnectorConfig config = new DiscordSourceConnectorConfig(props);

        assertEquals("18446744073709551615", config.channelId());
    }

    @Test
    void channelIdLargerThanUnsignedLongIsRejected() {
        Map<String, String> props = validProps();
        props.put(DiscordSourceConnectorConfig.CHANNEL_ID_CONFIG, "99999999999999999999");

        assertThrows(ConfigException.class, () -> new DiscordSourceConnectorConfig(props));
    }

    @Test
    void tokenIsHiddenWhenConfigIsPrinted() {
        DiscordSourceConnectorConfig config = new DiscordSourceConnectorConfig(validProps());

        String printed = config.values().get(DiscordSourceConnectorConfig.TOKEN_CONFIG).toString();

        assertFalse(printed.contains("test-token"));
    }
}
