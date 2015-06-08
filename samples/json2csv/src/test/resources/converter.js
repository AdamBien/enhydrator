/*
 * #%L
 * enhydrator
 * %%
 * Copyright (C) 2014 Adam Bien
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
var Row = Java.type("com.airhacks.enhydrator.in.Row");

print(INPUT);
var results = JSON.parse(INPUT);

var languages = results.languages;
var numberOfLanguages = languages.length;

var header = results.headers;
for (var i = 0; i < numberOfLanguages; i++) {
    var row = new Row();
    var language = languages[i];
    for (key in language) {
        row.addColumn(0, "language", key);
        row.addColumn(1, "rank", language[key]);
    }
    ROWS.add(row);
}
ROWS;

