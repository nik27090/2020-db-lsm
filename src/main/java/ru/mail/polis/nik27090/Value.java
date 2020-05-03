package ru.mail.polis.nik27090;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

final class Value implements Comparable<Value> {
    private final long timestamp;
    private final boolean tombStone;
    private final ByteBuffer value;

    public Value(final long timestamp, @NotNull final ByteBuffer value) {
        this.timestamp = timestamp;
        this.value = value;
        this.tombStone = false;
    }

    public Value(final long timestamp, boolean tombStone) {
        this.timestamp = timestamp;
        this.tombStone = tombStone;
        this.value = null;
    }

    public boolean isTombStone() {
        return tombStone;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @NotNull
    public ByteBuffer getValue() {
        return value;
    }

    @Override
    public int compareTo(@NotNull Value o) {
        return Long.compare(o.timestamp, timestamp);
    }
}
