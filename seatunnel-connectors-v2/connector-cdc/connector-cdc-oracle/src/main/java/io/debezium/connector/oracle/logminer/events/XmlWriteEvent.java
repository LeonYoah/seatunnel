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

package io.debezium.connector.oracle.logminer.events;

import io.debezium.connector.oracle.Scn;
import io.debezium.relational.TableId;

import java.time.Instant;

/**
 * An event that represents a write operation of an XML document in the event stream.
 *
 * @author Chris Cranford
 */
public class XmlWriteEvent extends LogMinerEvent {

    private final String xml;
    private final int length;

    public XmlWriteEvent(LogMinerEventRow row, String xml, Integer length) {
        super(row);
        this.xml = xml;
        this.length = length;
    }

    public XmlWriteEvent(
            EventType eventType,
            Scn scn,
            TableId tableId,
            String rowId,
            String rsId,
            Instant changeTime,
            String xml,
            Integer length) {
        super(eventType, scn, tableId, rowId, rsId, changeTime);
        this.xml = xml;
        this.length = length;
    }

    public String getXml() {
        return xml;
    }

    public Integer getLength() {
        return length;
    }

    @Override
    public String toString() {
        return "XmlWriteEvent{" + "xml='" + xml + '\'' + "} " + super.toString();
    }
}
