package ru.mail.polis.nik27090;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

final class Value implements Comparable<Value> {
    private final long timestamp;
    private final boolean tombStone;
    private final ByteBuffer content;

    public Value(final long timestamp, @NotNull final ByteBuffer content) {
        this.timestamp = timestamp;
        this.content = content;
        this.tombStone = false;
    }

    public Value(final long timestamp, final boolean tombStone) {
        this.timestamp = timestamp;
        this.tombStone = tombStone;
        this.content = null;
    }

    public boolean isTombStone() {
        return tombStone;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @NotNull
    public ByteBuffer getContent() {
        return content;
    }

    @Override
    public int compareTo(@NotNull final Value o) {
        return Long.compare(o.timestamp, timestamp);
    }
}
