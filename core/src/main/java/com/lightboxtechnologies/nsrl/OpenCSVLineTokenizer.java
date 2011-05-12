/*
   Copyright 2011, Lightbox Technologies, Inc

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package com.lightboxtechnologies.nsrl;

import java.io.IOException;

import au.com.bytecode.opencsv.CSVParser;

/**
 * A {@link LineTokenizer} for NSRL record lines using opencsv.
 *
 * @author Joel Uckelman
 */
class OpenCSVLineTokenizer implements LineTokenizer {
  private final CSVParser parser = new CSVParser(',', '"');

  public String[] tokenize(String line) throws IOException {
    return parser.parseLine(line);
  }
}
