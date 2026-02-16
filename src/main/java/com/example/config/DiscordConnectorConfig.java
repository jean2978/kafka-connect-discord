package com.example.config;

import org.apache.kafka.common.config.ConfigDef;

public class DiscordConnectorConfig {

    static final String DISCORD_WEBHOOK_URL_CONFIG = "discord.webhook.url";
    static final String TOPIC = "topic";
    //TODO add more configuration parameters as needed


    
    static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(DISCORD_WEBHOOK_URL_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "The Discord webhook URL to send messages to.")
        .define(TOPIC, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "The Kafka topic to read messages from.");
    
}
