/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.hadoopcompatibility.mapred.driver;

import java.util.Arrays;

import org.apache.flink.test.hadoopcompatibility.HadoopTestBase;
import org.apache.flink.test.testdata.WordCountData;

public class HadoopDriverCustomGrouperITCase extends HadoopTestBase {

	protected String textPath;
	protected String resultPath;


	@Override
	protected void preSubmit() throws Exception {
		textPath = createTempFile("text.txt", WordCountData.TEXT);
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void postSubmit() throws Exception {

		String[] expected = WordCountData.COUNTS.split("\n");
		Arrays.sort(expected);
		StringBuilder aggExpected = new StringBuilder();
		
		// aggregate sums of all words that begin with the same letter...
		String first = null;
		int sum = 0;
		for(String s : expected) {
			if (first == null) {
				first = s.split(" ")[0];
				sum = Integer.parseInt(s.split(" ")[1]);
			} else {
				if (first.charAt(0) == s.charAt(0)) {
					sum += Integer.parseInt(s.split(" ")[1]);
				} else {
					aggExpected.append(first+" "+sum+"\n");
					first = s.split(" ")[0];
					sum = Integer.parseInt(s.split(" ")[1]);
				}
			}
		}
		aggExpected.append(first+" "+sum+"\n");

		compareResultsByLinesInMemory(aggExpected.toString(), resultPath + "/1");
	}

	@Override
	protected void testProgram() throws Exception {
		HadoopWordCountVariations.WordCountCustomGrouper.main(new String[]{textPath, resultPath});
	}
}


