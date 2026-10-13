package com.example;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.OffsetDateTime;
import java.util.Date;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import net.dv8tion.jda.api.entities.Guild;
import net.dv8tion.jda.api.entities.Message;
import net.dv8tion.jda.api.entities.User;
import net.dv8tion.jda.api.entities.channel.unions.MessageChannelUnion;

/**
 * JDA's {@link Message} is an interface that needs a live Discord connection, so these tests use Mockito mocks:
 * you decide what each getter returns with {@code when(...).thenReturn(...)}.
 */
@ExtendWith(MockitoExtension.class)
// LENIENT: setUp() stubs more than each test needs. Without it Mockito fails tests that leave stubs unused.
@MockitoSettings(strictness = Strictness.LENIENT)
class DiscordMessageSchemaTest {

    private static final OffsetDateTime CREATED = OffsetDateTime.parse("2026-01-02T03:04:05Z");

    @Mock private Message message;
    @Mock private MessageChannelUnion channel;
    @Mock private Guild guild;
    @Mock private User author;

    /** A plain guild text message from a human, with no attachments and no edits. */
    @BeforeEach
    void setUp() {
        when(message.getId()).thenReturn("111");
        when(message.getChannel()).thenReturn(channel);
        when(channel.getId()).thenReturn("222");
        when(message.isFromGuild()).thenReturn(true);
        when(message.getGuild()).thenReturn(guild);
        when(guild.getId()).thenReturn("333");
        when(message.getAuthor()).thenReturn(author);
        when(author.getId()).thenReturn("444");
        when(author.getName()).thenReturn("jeppe");
        when(author.isBot()).thenReturn(false);
        when(message.getContentRaw()).thenReturn("hello");
        when(message.getTimeCreated()).thenReturn(CREATED);
        when(message.getTimeEdited()).thenReturn(null);
        when(message.getAttachments()).thenReturn(List.of());
    }

    // Worked example
    @Test
    void mapsBasicFields() {
        Struct struct = DiscordMessageSchema.toStruct(message);

        assertEquals("111", struct.getString("id"));
        assertEquals("222", struct.getString("channel_id"));
        assertEquals("hello", struct.getString("content"));
        assertEquals(Date.from(CREATED.toInstant()), struct.get("created_at"));
        assertNull(struct.get("edited_at"));
    }

    @Test
    @Disabled("TODO")
    void mapsGuildAndAuthorFields() {
    }

    @Test
    @Disabled("TODO")
    void directMessageHasNullGuildId() {
        // Hint: override isFromGuild() to return false in this test. What happens if toStruct called getGuild()?
    }

    @Test
    @Disabled("TODO")
    void editedMessageHasEditedAt() {
    }

    @Test
    @Disabled("TODO")
    void attachmentUrlsAreIncludedInOrder() {
        // Hint: mock(Message.Attachment.class), stub getUrl(), and return a list of them from getAttachments().
    }

    @Test
    @Disabled("TODO")
    void structIsValidAgainstSchema() {
        // Hint: struct.validate() throws if a required field is null or has the wrong type.
        // Try it with a bot author, an empty message, and a DM. Is there an input that breaks it?
        // (What does getContentRaw() return for a message that only has an embed or a sticker?)
    }

    @SuppressWarnings("unused")
    private static Message.Attachment attachment(String url) {
        Message.Attachment attachment = mock(Message.Attachment.class);
        when(attachment.getUrl()).thenReturn(url);
        return attachment;
    }
}
