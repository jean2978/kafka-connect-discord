package com.example;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import com.example.config.DiscordSourceConnectorConfig;

class DiscordSourceConnectorTest {

    private static final Map<String, String> VALID_PROPS = Map.of(
        DiscordSourceConnectorConfig.TOKEN_CONFIG, "test-token",
        DiscordSourceConnectorConfig.CHANNEL_ID_CONFIG, "1026585783924174882",
        DiscordSourceConnectorConfig.TOPIC_CONFIG, "discord-messages");

    // Worked example
    @Test
    void taskClassIsTheDiscordTask() {
        assertEquals(DiscordSourceConnectorTask.class, new DiscordSourceConnector().taskClass());
    }

    @Test
    void startRejectsInvalidConfig() {
        DiscordSourceConnector connector = new DiscordSourceConnector();
        Map<String, String> props = Map.of(DiscordSourceConnectorConfig.TOPIC_CONFIG, "discord-messages");

        assertThrows(ConfigException.class, () -> connector.start(props));
    }

    @Test
    void taskConfigsPassesPropsThrough() {
        DiscordSourceConnector connector = new DiscordSourceConnector();
        connector.start(VALID_PROPS);

        assertEquals(List.of(VALID_PROPS), connector.taskConfigs(1));
    }

    @Test
    void onlyOneTaskEvenWhenMoreAreAllowed() {
        DiscordSourceConnector connector = new DiscordSourceConnector();
        connector.start(VALID_PROPS);

        assertEquals(1, connector.taskConfigs(5).size());
    }

    @Test
    void configDefContainsAllProperties() {
        Set<String> names = new DiscordSourceConnector().config().names();

        assertEquals(Set.of(
            DiscordSourceConnectorConfig.TOKEN_CONFIG,
            DiscordSourceConnectorConfig.CHANNEL_ID_CONFIG,
            DiscordSourceConnectorConfig.TOPIC_CONFIG,
            DiscordSourceConnectorConfig.READ_HISTORY_CONFIG), names);
    }

    @Test
    void versionIsUnknownWhenNotRunningFromJar() {
        assertEquals("unknown", new DiscordSourceConnector().version());
    }
}
