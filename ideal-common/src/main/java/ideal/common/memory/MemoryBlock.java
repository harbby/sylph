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
package ideal.common.memory;

public class MemoryBlock
        extends MemoryLocation
{
    private final long length;
    public int pageNumber = -1;

    public MemoryBlock(Object obj, long offset, long length)
    {
        super(obj, offset);
        this.length = length;
    }

    /**
     * Returns the size of the memory block.
     */
    public long size()
    {
        return length;
    }

    /**
     * Fills the memory block with the specified byte value.
     */
    public void fill(byte value)
    {
        Platform.getUnsafe().setMemory(obj, offset, length, value);
    }
}
