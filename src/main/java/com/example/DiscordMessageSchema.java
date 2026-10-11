package com.example;

import java.time.OffsetDateTime;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import net.dv8tion.jda.api.entities.Message;

/** Kafka Connect schema for a Discord message and the conversion from JDA's {@link Message}. */
public final class DiscordMessageSchema {

    public static final Schema KEY_SCHEMA = Schema.STRING_SCHEMA;

    public static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
        .name("com.example.discord.Message")
        .field("id", Schema.STRING_SCHEMA)
        .field("channel_id", Schema.STRING_SCHEMA)
        .field("guild_id", Schema.OPTIONAL_STRING_SCHEMA)
        .field("author_id", Schema.STRING_SCHEMA)
        .field("author_name", Schema.STRING_SCHEMA)
        .field("author_bot", Schema.BOOLEAN_SCHEMA)
        .field("content", Schema.STRING_SCHEMA)
        .field("created_at", Timestamp.SCHEMA)
        .field("edited_at", Timestamp.builder().optional().build())
        .field("attachment_urls", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
        .build();

    private DiscordMessageSchema() {
    }

    public static Struct toStruct(Message message) {
        List<String> attachmentUrls = message.getAttachments().stream()
            .map(Message.Attachment::getUrl)
            .collect(Collectors.toList());

        return new Struct(VALUE_SCHEMA)
            .put("id", message.getId())
            .put("channel_id", message.getChannel().getId())
            .put("guild_id", message.isFromGuild() ? message.getGuild().getId() : null)
            .put("author_id", message.getAuthor().getId())
            .put("author_name", message.getAuthor().getName())
            .put("author_bot", message.getAuthor().isBot())
            .put("content", message.getContentRaw())
            .put("created_at", toDate(message.getTimeCreated()))
            .put("edited_at", toDate(message.getTimeEdited()))
            .put("attachment_urls", attachmentUrls);
    }

    private static Date toDate(OffsetDateTime time) {
        return time != null ? Date.from(time.toInstant()) : null;
    }
}
