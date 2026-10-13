package com.example;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * The task is the most important class to test, and the hardest: {@code start()} calls {@code JDABuilder...build()}
 * itself, which needs a real bot token and a network connection. That's an untestable "hard-wired dependency".
 *
 * <p>Before these tests can run, give the task a seam where a test can swap in a fake. Some options, simplest first:
 * <ol>
 *   <li>Move the JDA creation into a {@code protected JDA createJda(String token, Object listener)} method, then
 *       override it in a test subclass to return {@code mock(JDA.class)}.</li>
 *   <li>Add a package-private constructor that takes a {@code Function<DiscordSourceConnectorConfig, JDA>}
 *       factory; the public no-arg constructor (which Connect needs) passes the real one.</li>
 *   <li>Wrap JDA behind your own small interface (e.g. {@code DiscordClient} with {@code history(after, limit)} and
 *       a live-message callback) and fake that interface. More code, but much easier to test than mocking JDA's
 *       fluent {@code MessageHistory.getHistoryAfter(...).limit(...).complete()} chain.</li>
 * </ol>
 * Pick one, refactor, and check that the connector still works against Discord with {@code podman compose up}.
 *
 * <p>The offset lookup goes through {@code context}, which you can mock without any refactoring:
 * <pre>{@code
 * OffsetStorageReader reader = mock(OffsetStorageReader.class);
 * when(reader.offset(anyMap())).thenReturn(Map.of("message_id", "500"));
 * SourceTaskContext context = mock(SourceTaskContext.class);
 * when(context.offsetStorageReader()).thenReturn(reader);
 * task.initialize(context);
 * }</pre>
 */
class DiscordSourceConnectorTaskTest {

    // --- start(): picking where to begin ---

    @Test
    @Disabled("TODO: needs a seam for JDA first")
    void resumesAfterCommittedOffset() {
        // Committed offset "500" -> first poll() reads history after 500.
    }

    @Test
    @Disabled("TODO: needs a seam for JDA first")
    void readsHistoryFromBeginningWhenEnabledAndNoOffset() {
    }

    @Test
    @Disabled("TODO: needs a seam for JDA first")
    void goesStraightToLiveWhenHistoryDisabledAndNoOffset() {
    }

    @Test
    @Disabled("TODO: needs a seam for JDA first")
    void committedOffsetWinsOverReadHistory() {
        // The README promises: once an offset exists, discord.read.history is ignored.
    }

    @Test
    @Disabled("TODO: needs a seam for JDA first")
    void unknownChannelFailsWithClearError() {
        // jda.getChannelById(...) returns null -> ConnectException with a helpful message.
    }

    // --- poll(): catch-up phase ---

    @Test
    @Disabled("TODO: needs a seam for JDA first")
    void fullHistoryPageKeepsCatchingUp() {
        // 100 messages returned -> the next poll() asks for history again, after the last ID.
    }

    @Test
    @Disabled("TODO: needs a seam for JDA first")
    void shortHistoryPageSwitchesToLive() {
    }

    @Test
    @Disabled("TODO: needs a seam for JDA first")
    void emptyChannelReturnsNull() {
        // Connect treats null from poll() as "nothing right now". An empty list would also be accepted. Which does the code return?
    }

    // --- poll(): live phase and dedup ---

    @Test
    @Disabled("TODO: needs a seam for JDA first")
    void messageSentDuringCatchUpIsNotEmittedTwice() {
        // The same message arrives in a history page AND on the live queue. It must appear exactly once.
        // This is the trickiest logic in the task. Worth testing well.
    }

    @Test
    @Disabled("TODO: needs a seam for JDA first")
    void liveMessagesAreSortedById() {
    }

    @Test
    @Disabled("TODO: needs a seam for JDA first")
    void liveBatchIsCappedAt500() {
    }

    @Test
    @Disabled("TODO: needs a seam for JDA first")
    void messagesFromOtherChannelsAreIgnored() {
        // Hint: you'll need to capture the ListenerAdapter the task registers and call onMessageReceived yourself.
        // Mockito's ArgumentCaptor helps here.
    }

    // --- records ---

    @Test
    @Disabled("TODO: needs a seam for JDA first")
    void recordHasPartitionOffsetTopicKeyAndTimestamp() {
        // Check sourcePartition() == {channel_id: ...}, sourceOffset() == {message_id: ...},
        // topic(), key() == channel ID, timestamp() == message creation time in millis.
    }

    @Test
    @Disabled("TODO: needs a seam for JDA first")
    void idsAboveSignedLongMaxAreHandled() {
        // Discord IDs are unsigned. Sorting uses Comparator.comparingLong (signed!) but dedup uses
        // Long.compareUnsigned. Do they agree for real Discord IDs? Can they ever disagree?
    }

    // --- stop() ---

    @Test
    @Disabled("TODO")
    void stopBeforeStartDoesNotThrow() {
        // Connect may call stop() even if start() failed. This one needs no refactoring: try it now.
    }

    // Helpers you'll probably want. Change or delete them freely.
    @SuppressWarnings("unused")
    private static SourceTaskContext contextWithOffset(Map<String, Object> offset) {
        OffsetStorageReader reader = mock(OffsetStorageReader.class);
        when(reader.offset(org.mockito.ArgumentMatchers.anyMap())).thenReturn(offset);
        SourceTaskContext context = mock(SourceTaskContext.class);
        when(context.offsetStorageReader()).thenReturn(reader);
        return context;
    }

    @SuppressWarnings("unused")
    private static void assertNoRecords(List<SourceRecord> records) {
        assertNull(records);
    }

    @SuppressWarnings("unused")
    private static void assertRecordCount(int expected, List<SourceRecord> records) {
        assertEquals(expected, records.size());
    }
}
