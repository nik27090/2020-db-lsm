package ru.mail.polis.nik27090;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Client;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;

public class SSTable implements Table {

    private static final String CANT_READ = "Cant read file";
    private static final Logger log = LoggerFactory.getLogger(Client.class);

    private final FileChannel channel;
    private final int amountElement;
    private final long indexStart;

    private long iterPosition;

    /**
     * Stores data in bit representation.
     * file structure: [row] ... [index] ... number of row.
     * row - keyLen, keyBytes, timeStamp, isAlive, valueLen, valueBytes.
     * index - start point position of row.
     *
     * @param file file created using serialize()
     */
    SSTable(@NotNull final File file) throws IOException {
        this.channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        final long size = channel.size();
        final ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);

        channel.read(byteBuffer, size - Integer.BYTES);
        this.amountElement = byteBuffer.getInt(0);

        this.indexStart = size - Integer.BYTES - Long.BYTES * amountElement;
    }

    /**
     * Converts MemTable to SSTable and writes it on disk.
     *
     * @param file     temporary file for recording
     * @param iterator contains all Cell of MemTable
     * @param size     number of records in MemTable
     */
    public static void serialize(final File file, final Iterator<Cell> iterator, final int size) {
        try (FileChannel fileChannel = FileChannel.open(file.toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

            final ArrayList<ByteBuffer> bufOffsetArray = new ArrayList<>();
            while (iterator.hasNext()) {
                final Cell cell = iterator.next();
                //указатель
                bufOffsetArray.add(longToByteBuffer(fileChannel.position()));
                //keySize
                fileChannel.write(intToByteBuffer(cell.getKey().limit()));
                //keyBytes
                fileChannel.write(cell.getKey());
                //timestamp
                fileChannel.write(longToByteBuffer(cell.getValue().getTimestamp()));
                //isAlive
                if (cell.getValue().isTombStone()) {
                    fileChannel.write(ByteBuffer.wrap(new byte[]{1}));
                } else {
                    fileChannel.write(ByteBuffer.wrap(new byte[]{0}));
                    fileChannel.write(intToByteBuffer(cell.getValue().getContent().limit()));
                    fileChannel.write(cell.getValue().getContent());
                }
            }
            //количество элементов
            bufOffsetArray.add(intToByteBuffer(size));
            for (final ByteBuffer buff : bufOffsetArray) {
                fileChannel.write(buff);
            }
        } catch (IOException e) {
            log.warn("Cant write file", e);
        }
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) throws IOException {
        return new Iterator<>() {
            final ByteBuffer key = from.duplicate();
            boolean firstIn = true;
            Cell firstElement = findElement(key);

            @Override
            public boolean hasNext() {
                if (firstElement == null) {
                    return false;
                }
                return indexStart >= iterPosition;
            }

            @Override
            public Cell next() {
                if (firstIn) {
                    firstIn = false;
                    return firstElement;
                }
                assert hasNext();
                return nextElement();
            }
        };
    }

    private Cell findElement(final ByteBuffer from) throws IOException {
        final ByteBuffer key = from.rewind();
        int low = 0;
        int high = amountElement - 1;
        int mid;
        while (low <= high) {
            mid = low + (high - low) / 2;
            final ByteBuffer midKey = getKeyByOrder(mid).rewind();

            final int compare = midKey.compareTo(key);
            if (compare < 0) {
                low = mid + 1;
            } else if (compare > 0) {
                high = mid - 1;
            } else {
                return getCell(midKey);
            }
        }
        return null;
    }

    private ByteBuffer getKeyByOrder(final int order) {
        final ByteBuffer bbIndex = ByteBuffer.allocate(Long.BYTES);
        try {
            channel.read(bbIndex, indexStart + Long.BYTES * order);
        } catch (IOException e) {
            log.warn(CANT_READ, e);
        }

        iterPosition = bbIndex.getLong(0);
        //Not duplicate
        final ByteBuffer bbKeySize = ByteBuffer.allocate(Integer.BYTES);
        try {
            channel.read(bbKeySize, iterPosition);
        } catch (IOException e) {
            log.warn(CANT_READ, e);
        }

        iterPosition += bbKeySize.limit();
        final ByteBuffer bbKeyValue = ByteBuffer.allocate(bbKeySize.getInt(0));
        try {
            channel.read(bbKeyValue, iterPosition);
        } catch (IOException e) {
            log.warn(CANT_READ, e);
        }

        iterPosition += bbKeyValue.limit();
        return bbKeyValue;
    }

    private Cell getCell(final @NotNull ByteBuffer key) {
        final ByteBuffer bbTimeStamp = ByteBuffer.allocate(Long.BYTES);

        //Not duplicated
        try {
            channel.read(bbTimeStamp, iterPosition);
        } catch (IOException e) {
            log.warn(CANT_READ, e);
        }
        iterPosition += bbTimeStamp.limit();
        final ByteBuffer bbIsAlive = ByteBuffer.allocate(1);
        try {
            channel.read(bbIsAlive, iterPosition);
        } catch (IOException e) {
            log.warn(CANT_READ, e);
        }
        final byte isAlive = bbIsAlive.get(0);
        iterPosition += bbIsAlive.limit();
        ByteBuffer bbValueContent;
        if (isAlive == 1) {
            return new Cell(key.rewind(), new Value(bbTimeStamp.getLong(0), true));
        } else {
            final ByteBuffer bbValueSize = ByteBuffer.allocate(Integer.BYTES);
            try {
                channel.read(bbValueSize, iterPosition);
            } catch (IOException e) {
                log.warn(CANT_READ, e);
            }
            iterPosition += bbValueSize.limit();
            bbValueContent = ByteBuffer.allocate(bbValueSize.getInt(0));
            try {
                channel.read(bbValueContent, iterPosition);
            } catch (IOException e) {
                log.warn(CANT_READ, e);
            }
            iterPosition += bbValueContent.limit();
            return new Cell(key.rewind(), new Value(bbTimeStamp.getLong(0), bbValueContent.rewind()));
        }
    }

    private Cell nextElement() {
        final ByteBuffer bbKeySize = ByteBuffer.allocate(Integer.BYTES);
        try {
            channel.read(bbKeySize, iterPosition);
        } catch (IOException e) {
            log.warn(CANT_READ, e);
        }

        final ByteBuffer bbKeyValue = ByteBuffer.allocate(bbKeySize.getInt(0));
        iterPosition += bbKeySize.limit();
        try {
            channel.read(bbKeyValue, iterPosition);
        } catch (IOException e) {
            log.warn(CANT_READ, e);
        }

        return getCell(bbKeyValue);
    }

    /**
     * closes the channel.
     */
    public void closeChannel() {
        try {
            channel.close();
        } catch (IOException e) {
            log.warn("Cant close channel", e);
        }
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        throw new UnsupportedOperationException("Unsupported method!");
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        throw new UnsupportedOperationException("Unsupported method!");
    }

    private static ByteBuffer longToByteBuffer(final long value) {
        return ByteBuffer.allocate(Long.BYTES).putLong(value).rewind();
    }

    private static ByteBuffer intToByteBuffer(final int value) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(value).rewind();
    }
}
