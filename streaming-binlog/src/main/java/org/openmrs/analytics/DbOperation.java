// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.openmrs.analytics;

/**
 * Enum to translate DB operations - debezium
 */
public enum DbOperation {

    READ("r"),
    CREATE("c"),
    UPDATE("u"),
    DELETE("d");

    private final String code; // debezium codes

    private DbOperation(String code) {
        this.code = code;
    }

    public String code() {
        return this.code;
    }

    public static DbOperation forCode(String code) {
        DbOperation[] var1 = values();
        int var2 = var1.length;

        for(int var3 = 0; var3 < var2; ++var3) {
            DbOperation op = var1[var3];
            if (op.code().equalsIgnoreCase(code)) {
                return op;
            }
        }
        return null;
    }
}
