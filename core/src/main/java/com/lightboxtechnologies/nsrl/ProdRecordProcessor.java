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
 * A {@link RecordProcessor} for NSRL product records.
 *
 * @author Joel Uckelman
 */
class ProdRecordProcessor implements RecordProcessor<ProdData> {
  public ProdData process(String[] col) throws BadDataException {
    if (col.length < 7) {
      throw new BadDataException("too few columns");
    }
    else if (col.length > 7) {
      throw new BadDataException("too many columns");
    }

    Integer code = null;
    try {
      code = Integer.valueOf(col[0]);
    }
    catch (NumberFormatException e) {
      throw new BadDataException(e);
    }

    if (code < 0) {
      throw new BadDataException("code < 0");
    }

    return new ProdData(code, col[1], col[2], col[3], col[4], col[5], col[6]);
  }
}
