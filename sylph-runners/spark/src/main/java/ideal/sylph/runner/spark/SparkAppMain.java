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

import ideal.common.base.Serializables;
import ideal.sylph.spi.App;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.StreamingContext;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * spark main input
 */
public final class SparkAppMain
{
    private SparkAppMain() {}

    public static void main(String[] args)
            throws Exception
    {
        System.out.println("spark on yarn app starting...");

        byte[] bytes = Files.readAllBytes(Paths.get(new File("job_handle.byt").toURI()));
        @SuppressWarnings("unchecked")
        SparkJobHandle<App<?>> sparkJobHandle = (SparkJobHandle<App<?>>) Serializables.byteToObject(bytes);

        App<?> app = requireNonNull(sparkJobHandle, "sparkJobHandle is null").getApp().get();
        app.build();
        Object appContext = app.getContext();
        if (appContext instanceof SparkSession) {
            checkArgument(((SparkSession) appContext).streams().active().length > 0, "no stream pipeline");
            ((SparkSession) appContext).streams().awaitAnyTermination();
        }
        else if (appContext instanceof StreamingContext) {
            ((StreamingContext) appContext).start();
            ((StreamingContext) appContext).awaitTermination();
        }
    }
}
