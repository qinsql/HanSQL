/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.hansql.test;

import org.lealone.main.Lealone;
import org.lealone.main.config.Config;

//加上-Xbootclasspath/a:../hansql-function/target/generated-sources;../hansql-function/src/main/java
public class HanEngineStart {

    public static void main(String[] args) {
        Config.setProperty("config", "lealone-test.yaml");
        Lealone.main(args);
    }

}
