/*
 * Copyright (C) 2018 The Sylph Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ideal.common.base;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.List;

public class Files
{
    private Files() {}

    public static List<File> listFiles(File input, boolean recursive)
    {
        List<File> files = new ArrayList<>();
        scanFiles(input, recursive, (fileName) -> true, files);
        return files;
    }

    public static List<File> listFiles(File input, boolean recursive, FileFilter filter)
    {
        List<File> files = new ArrayList<>();
        scanFiles(input, recursive, filter, files);
        return files;
    }

    private static void scanFiles(File input, boolean recursive, FileFilter filter, List<File> list)
    {
        if (input.isDirectory()) {
            File[] tmp = input.listFiles(filter);
            if (tmp == null) {
                return;
            }

            for (File it : tmp) {
                if (it.isFile()) {
                    list.add(it);
                }
                else if (recursive) {  //Directory()
                    scanFiles(it, recursive, filter, list);
                }
            }
        }
        else {
            list.add(input);
        }
    }
}
