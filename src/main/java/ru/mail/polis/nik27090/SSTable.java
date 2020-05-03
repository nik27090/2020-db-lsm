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

    private static final Logger log = LoggerFactory.getLogger(Client.class);

    private final FileChannel channel;
    //количесвтой байт
    private final long size;
    private final int amountElement;
    private final long indexStart;

    private long iterPosition;


    SSTable(@NotNull final File file) throws IOException {
        this.channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        this.size = channel.size();
        ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);

        channel.read(byteBuffer, size - Integer.BYTES);
        this.amountElement = byteBuffer.getInt(0);

        this.indexStart = size - Integer.BYTES - Long.BYTES * amountElement;
        //rows
        //index : rows x len
        //[row]
        // row == keyLeaght, keyBytes, timestamp, isAlive, [valueLenght, valueBytes]
    }

    public static void serialize(File file, Iterator<Cell> iterator, int size) throws IOException {
        FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
        ArrayList<ByteBuffer> bufOffsetArray = new ArrayList<>();
        while (iterator.hasNext()) {
            try {
                Cell cell = iterator.next();
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
                    fileChannel.write(intToByteBuffer(cell.getValue().getValue().limit()));
                    fileChannel.write(cell.getValue().getValue());
                }
            } catch (IOException e) {
                System.out.println("Error: " + e);
            }
        }
        //количество элементов
        bufOffsetArray.add(intToByteBuffer(size));
        for (ByteBuffer buff : bufOffsetArray) {
            fileChannel.write(buff);
        }
        fileChannel.close();
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull ByteBuffer from) {
        return new Iterator<>() {
            boolean firstIn = true;
            Cell firstElement = findElement(from);

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

    private Cell findElement(ByteBuffer from) {
        from = from.rewind();
        int low = 0;
        int high = amountElement - 1;
        int mid;
        while (low <= high) {
            mid = low + (high - low) / 2;
            ByteBuffer midKey = getKeyByOrder(mid).rewind();

            int compare = midKey.compareTo(from);
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

    //order - 1й, 2й, 3й и тд
    private ByteBuffer getKeyByOrder(int order) {
        ByteBuffer bbKeyValue = null;
        try {
            ByteBuffer bbIndex = ByteBuffer.allocate(Long.BYTES);
            channel.read(bbIndex, indexStart + Long.BYTES * order);

            iterPosition = bbIndex.getLong(0);
            ByteBuffer bbKeySize = ByteBuffer.allocate(Integer.BYTES);
            channel.read(bbKeySize, iterPosition);

            iterPosition += bbKeySize.limit();
            bbKeyValue = ByteBuffer.allocate(bbKeySize.getInt(0));
            channel.read(bbKeyValue, iterPosition);

            iterPosition += bbKeyValue.limit();
            return bbKeyValue;
        } catch (IOException e) {
            log.error("getKeyByOrder", e);
        }
        return bbKeyValue;
    }

    //должен вызываться только в nextElement() и findElement()
    private Cell getCell(ByteBuffer key) {
        Cell result = null;
        try {
            ByteBuffer bbTimeStamp = ByteBuffer.allocate(Long.BYTES);
            channel.read(bbTimeStamp, iterPosition);

            iterPosition += bbTimeStamp.limit();
            ByteBuffer bbIsAlive = ByteBuffer.allocate(1);
            channel.read(bbIsAlive, iterPosition);

            byte isAlive = bbIsAlive.get(0);
            iterPosition += bbIsAlive.limit();

            ByteBuffer bbValueContent;

            if (isAlive == 1) {
                result = new Cell(key.rewind(), new Value(bbTimeStamp.getLong(0), true));
            } else {
                ByteBuffer bbValueSize = ByteBuffer.allocate(Integer.BYTES);
                channel.read(bbValueSize, iterPosition);

                iterPosition += bbValueSize.limit();

                bbValueContent = ByteBuffer.allocate(bbValueSize.getInt(0));
                channel.read(bbValueContent, iterPosition);

                iterPosition += bbValueContent.limit();

                result = new Cell(key.rewind(), new Value(bbTimeStamp.getLong(0), bbValueContent.rewind()));
            }
        } catch (IOException e) {
            log.warn("getCell", e);
        }
        return result;
    }

    private Cell nextElement() {
        ByteBuffer bbKeyValue = null;
        try {
            ByteBuffer bbKeySize = ByteBuffer.allocate(Integer.BYTES);
            channel.read(bbKeySize, iterPosition);

            bbKeyValue = ByteBuffer.allocate(bbKeySize.getInt(0));
            iterPosition += bbKeySize.limit();
            channel.read(bbKeyValue, iterPosition);
        } catch (IOException e) {
            log.warn("nextElement", e);
        }
        return getCell(bbKeyValue);
    }

    public void closeChannel() {
        try {
            channel.close();
        } catch (IOException e) {
            log.warn("Cant close channel", e);
        }
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        throw new UnsupportedOperationException("Unsupported method!");
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        throw new UnsupportedOperationException("Unsupported method!");
    }

    private static ByteBuffer longToByteBuffer(long value) {
        return ByteBuffer.allocate(Long.BYTES).putLong(value).rewind();
    }

    private static ByteBuffer intToByteBuffer(int value) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(value).rewind();
    }
}
