package com.lightboxtechnologies.spectrum;

public interface FsEntryFilter {
  boolean accept(byte[] id, FsEntry entry);
}
