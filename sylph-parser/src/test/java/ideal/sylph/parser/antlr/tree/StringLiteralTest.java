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
package ideal.sylph.parser.antlr.tree;

import org.junit.Assert;
import org.junit.Test;

public class StringLiteralTest {

    @Test
    public void testGetValue() {
        NodeLocation nodeLocation = new NodeLocation(1, 1);
        StringLiteral stringLiteral1 = new StringLiteral(nodeLocation, "foo");

        Assert.assertEquals("foo", stringLiteral1.getValue());
    }

    @Test
    public void testEquals() {
        NodeLocation nodeLocation = new NodeLocation(1, 1);
        StringLiteral stringLiteral1 = new StringLiteral(nodeLocation, "foo");
        StringLiteral stringLiteral2 = new StringLiteral(nodeLocation, "bar");

        Assert.assertTrue(stringLiteral1.equals(stringLiteral1));

        Assert.assertFalse(stringLiteral1.equals(null));
        Assert.assertFalse(stringLiteral1.equals("foo"));
        Assert.assertFalse(stringLiteral1.equals(stringLiteral2));
    }

    @Test
    public void testToString() {
        NodeLocation nodeLocation = new NodeLocation(1, 1);
        StringLiteral stringLiteral1 = new StringLiteral(nodeLocation, "foo");
        Assert.assertEquals("\'foo\'", stringLiteral1.toString());
    }

    @Test
    public void testFormatStringLiteral() {
        Assert.assertEquals("'|'",
                StringLiteral.formatStringLiteral("\u007C"));
        Assert.assertEquals("U&'\\0000\\FFFF'",
                StringLiteral.formatStringLiteral("\u0000\uffff"));
    }
}
