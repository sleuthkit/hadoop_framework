/*
   Copyright 2011 Basis Technology Corp.

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

package org.sleuthkit.hadoop;

import java.io.File;

/**
 * @author Mark Zand
 *
 *  Recursively process files/directories
 */
public class FileProcessor {
	private Visitor<File> visitor;

	public FileProcessor(Visitor<File> visitor) {
		this.visitor = visitor;
	}
	public void process(File file) {
		if (file.isDirectory()) {
			File[] files = file.listFiles();
			for (File f : files) {
				process(f);
			}
		}
		else if (file.isFile()) {
			visitor.visit(file);
		}
	}
}
