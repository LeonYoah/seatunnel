/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.debezium.connector.oracle.logminer.processor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LobWriteEvent;
import io.debezium.connector.oracle.logminer.processor.TransactionCommitConsumer.LobFragment;
import io.debezium.connector.oracle.logminer.processor.TransactionCommitConsumer.LobUnderConstruction;
import io.debezium.connector.oracle.logminer.processor.TransactionCommitConsumer.XmlFragment;
import io.debezium.connector.oracle.logminer.processor.TransactionCommitConsumer.XmlUnderConstruction;
import io.debezium.relational.TableId;

import java.time.Instant;

public class TransactionCommitConsumerTest {

    private static final String EMOJI = "\uD83D\uDE00";
    private static final TableId TABLE = TableId.parse("DEBEZIUM.LOB_TYPES");

    @Test
    public void keepsEveryEmojiWhenLogMinerLengthCountsCharacters() {
        String four = repeat(EMOJI, 4);
        LobUnderConstruction lob = new LobUnderConstruction();
        lob.add(fragment(four, 0, 4));

        String merged = (String) lob.merge();
        Assertions.assertEquals(four, merged);
        Assertions.assertEquals(4, merged.codePointCount(0, merged.length()));
        Assertions.assertFalse(Character.isHighSurrogate(merged.charAt(merged.length() - 1)));
    }

    @Test
    public void mergesSequentialEmojiChunks() {
        String first = repeat(EMOJI, 4);
        String second = repeat(EMOJI, 4);
        LobUnderConstruction lob = new LobUnderConstruction();
        lob.add(fragment(first, 0, 4));
        lob.add(fragment(second, 4, 4));

        Assertions.assertEquals(first + second, lob.merge());
    }

    @Test
    public void mergesEmojiWhoseAmountIsUtf16UnitsWithFollowingAscii() {
        LobUnderConstruction lob = new LobUnderConstruction();
        lob.add(fragment("aaa", 0, 3));
        lob.add(fragment(EMOJI, 3, 2));
        lob.add(fragment("bbb", 5, 3));

        String merged = (String) lob.merge();
        String expected = "aaa" + EMOJI + "bbb";
        Assertions.assertEquals(expected, merged);
        Assertions.assertEquals(expected.length(), merged.length());
        Assertions.assertEquals(-1, merged.indexOf(' '));
    }

    @Test
    public void abutsAsciiWhenTheNextOffsetJumpsByTheEmojiUtf16Length() {
        LobUnderConstruction lob = new LobUnderConstruction();
        lob.add(fragment(EMOJI, 0, 1));
        lob.add(fragment("abc", 2, 3));

        String merged = (String) lob.merge();
        Assertions.assertEquals(EMOJI + "abc", merged);
        Assertions.assertEquals((EMOJI + "abc").length(), merged.length());
    }

    @Test
    public void keepsARealHoleBetweenAsciiChunks() {
        LobUnderConstruction lob = new LobUnderConstruction();
        lob.add(fragment("aa", 0, 2));
        lob.add(fragment("bb", 4, 2));

        Assertions.assertEquals("aa  bb", lob.merge());
    }

    @Test
    public void reselectsWhenAmountMatchesNeitherCodePointsNorUtf16Length() {
        LobUnderConstruction lob = new LobUnderConstruction();
        lob.add(fragment(EMOJI + EMOJI, 0, 3));
        lob.add(fragment("a", 3, 1));

        Assertions.assertSame(OracleValueConverters.UNAVAILABLE_VALUE, lob.merge());
    }

    @Test
    public void truncatesAsciiByDeclaredCharacterLength() {
        LobUnderConstruction lob = new LobUnderConstruction();
        lob.add(fragment("abcdef", 0, 3));

        Assertions.assertEquals("abc", lob.merge());
    }

    @Test
    public void truncatesAnEmojiBufferToCompleteCharacters() {
        LobUnderConstruction lob = new LobUnderConstruction();
        lob.add(fragment(repeat(EMOJI, 6), 0, 4));

        String merged = (String) lob.merge();
        Assertions.assertEquals(repeat(EMOJI, 4), merged);
        Assertions.assertEquals(8, merged.length());
    }

    @Test
    public void absorbsAnOverlappingEmojiWriteByCodePoint() {
        String grin = EMOJI;
        String smile = "\uD83D\uDE01";
        LobUnderConstruction lob = new LobUnderConstruction();
        lob.add(fragment(repeat(grin, 6), 0, 6));
        lob.add(fragment(repeat(smile, 2), 2, 2));

        Assertions.assertEquals(repeat(grin, 2) + repeat(smile, 2) + repeat(grin, 2), lob.merge());
    }

    @Test
    public void truncatesBlobWritesByByteLength() {
        LobUnderConstruction lob = new LobUnderConstruction();
        lob.add(fragment("HEXTORAW('01020304')", 0, 2));

        Assertions.assertArrayEquals(new byte[] {0x01, 0x02}, (byte[]) lob.merge());
    }

    @Test
    public void assemblesXmlFragmentsInOrder() {
        XmlUnderConstruction xml = XmlUnderConstruction.fromInitialValue(null);
        Assertions.assertNull(xml.merge());

        xml.add(new XmlFragment("<root>"));
        xml.add(new XmlFragment(EMOJI + "</root>"));
        Assertions.assertEquals("<root>" + EMOJI + "</root>", xml.merge());
    }

    private static LobFragment fragment(String data, int offset, int length) {
        return new LobFragment(
                new LobWriteEvent(
                        EventType.LOB_WRITE,
                        Scn.valueOf(1),
                        TABLE,
                        "AAABBB",
                        "0x1.2.3",
                        Instant.EPOCH,
                        data,
                        offset,
                        length));
    }

    private static String repeat(String value, int times) {
        StringBuilder builder = new StringBuilder(value.length() * times);
        for (int i = 0; i < times; i++) {
            builder.append(value);
        }
        return builder.toString();
    }
}
