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

import org.junit.Test;

import static org.fusesource.jansi.Ansi.Color.GREEN;
import static org.fusesource.jansi.Ansi.Color.RED;
import static org.fusesource.jansi.Ansi.Color.YELLOW;
import static org.fusesource.jansi.Ansi.ansi;

public class JansiTest
{
    @Test
    public void colorLineTest()
    {
        int i = 1 % 7;
        System.out.println(ansi().eraseScreen().fg(RED).a(i).fg(YELLOW).a(" World").reset());
        System.out.println(ansi().eraseScreen().fg(RED).a(i).fg(GREEN).a(" World").reset());
        System.out.println(123);
        System.out.println(ansi().eraseScreen().render("@|red hello|@"));
    }
}
