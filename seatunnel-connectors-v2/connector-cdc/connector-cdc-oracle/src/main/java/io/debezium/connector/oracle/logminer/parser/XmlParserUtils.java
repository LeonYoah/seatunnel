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

package io.debezium.connector.oracle.logminer.parser;

import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;

/**
 * Helpers for Oracle LogMiner XML redo. Matches Debezium {@code XmlParserUtils}: binary {@code
 * XML_WRITE} rows have {@code INFO} ending with {@code not re-executable}.
 *
 * @see XmlWriteParser
 */
public final class XmlParserUtils {

    private XmlParserUtils() {}

    /**
     * Returns whether the XML event is serialized using binary format.
     *
     * @param event the event, should not be {@code null}
     * @return {@code true} if the XML document is serialized as binary
     */
    public static boolean isXmlSerializedAsBinary(LogMinerEventRow event) {
        return isXmlSerializedAsBinary(event.getInfo());
    }

    /**
     * Same check as {@link #isXmlSerializedAsBinary(LogMinerEventRow)} when only {@code INFO} is
     * available. {@code null} is treated as not binary so 11g/21c document-text redo is assembled.
     */
    public static boolean isXmlSerializedAsBinary(String info) {
        return info != null && info.endsWith("not re-executable");
    }
}
