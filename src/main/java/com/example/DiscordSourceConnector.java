package com.example;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import com.example.config.DiscordSourceConnectorConfig;

public class DiscordSourceConnector extends SourceConnector {

    private Map<String, String> props;

    @Override
    public String version() {
        return Version.get();
    }

    @Override
    public ConfigDef config() {
        return DiscordSourceConnectorConfig.CONFIG_DEF;
    }

    @Override
    public void start(Map<String, String> props) {
        // Fail fast on invalid config before any task is started
        new DiscordSourceConnectorConfig(props);
        this.props = props;
    }

    @Override
    public void stop() {
    }

    @Override
    public Class<? extends Task> taskClass() {
        return DiscordSourceConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // A single channel can only be read in order by one task
        return List.of(props);
    }
}
