/*
 * For licensing see accompanying LICENSE file.
 * Copyright (C) 2024 Apple Inc. All Rights Reserved.
 */

package com.apple.musicFeed.example.models;

import java.util.Map;

/* Minimal example POJO for reading a MediaAPI response for requesting Parts
 */
public class PartResponse {
    public PartResponseResources resources;
    public String nextLink;

    String getNextLink() {
        return nextLink;
    }

    void setNextLink(final String nextLink) {
        this.nextLink = nextLink;
    }

    public PartResponseResources getPartResponseResources() {
        return resources;
    }

    public void setPartResponseResources(final PartResponseResources partResponseResources) {
        this.resources = partResponseResources;
    }

    public static class PartResponseResources {
        public Map<String, Part> parts;

        public Map<String, Part> getParts() {
            return parts;
        }

        public void setParts(final Map<String, Part> parts) {
            this.parts = parts;
        }
    }
}
