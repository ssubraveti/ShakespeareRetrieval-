package com.invertedindex;

import java.util.HashMap;
import java.util.Map;

public class LookupData {


    Map<String, Long> metadata;

    LookupData(long offset, long length, long ctf, long df) {
        metadata = new HashMap<String, Long>();
        this.metadata.put("offset", offset);
        this.metadata.put("length", length);
        this.metadata.put("ctf", ctf);
        this.metadata.put("df", df);
    }

    long getCtf() {
        return this.metadata.get("ctf");
    }

    long getDf() {
        return this.metadata.get("df");
    }

    long getOffset() {
        return this.metadata.get("offset");
    }

    long getLength() {
        return this.metadata.get("length");
    }

    Map<String, Long> getMetadata() {
        return metadata;
    }
}
