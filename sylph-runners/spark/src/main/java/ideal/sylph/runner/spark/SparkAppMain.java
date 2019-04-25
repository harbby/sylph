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
package ideal.sylph.runner.spark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.StreamingContext;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * spark on yarn main Class
 */
public final class SparkAppMain
{
    private SparkAppMain() {}

    public static void main(String[] args)
            throws Exception
    {
        System.out.println("spark on yarn app starting...");

        @SuppressWarnings("unchecked")
        Supplier<?> sparkJobHandle = (Supplier<?>) byteToObject(new FileInputStream("job.graph"));

        Object appContext = requireNonNull(sparkJobHandle, "sparkJobHandle is null").get();
        if (appContext instanceof SparkSession) {
            checkArgument(((SparkSession) appContext).streams().active().length > 0, "no stream pipeline");
            ((SparkSession) appContext).streams().awaitAnyTermination();
        }
        else if (appContext instanceof StreamingContext) {
            ((StreamingContext) appContext).start();
            ((StreamingContext) appContext).awaitTermination();
        }
    }

    private static Object byteToObject(InputStream inputStream)
            throws IOException, ClassNotFoundException
    {
        try (ObjectInputStream oi = new ObjectInputStream(inputStream)
        ) {
            return oi.readObject();
        }
    }
}
