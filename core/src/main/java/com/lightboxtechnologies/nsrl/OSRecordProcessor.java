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

/**
 * A {@link RecordProcessor} for NSRL operating system records.
 *
 * @author Joel Uckelman
 */
class OSRecordProcessor implements RecordProcessor<OSData> {
  public OSData process(String[] col) throws BadDataException {
    if (col.length < 4) {
      throw new BadDataException("too few columns");
    }
    else if (col.length > 4) {
      throw new BadDataException("too many columns");
    }

    return new OSData(col[0], col[1], col[2], col[3]);
  }
}
