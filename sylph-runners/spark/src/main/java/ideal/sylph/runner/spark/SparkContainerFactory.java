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

import com.github.harbby.gadtry.ioc.IocFactory;
import com.github.harbby.gadtry.jvm.JVMLauncher;
import com.github.harbby.gadtry.jvm.JVMLaunchers;
import com.github.harbby.gadtry.jvm.VmFuture;
import ideal.sylph.runner.spark.yarn.SparkAppLauncher;
import ideal.sylph.runtime.local.LocalContainer;
import ideal.sylph.runtime.yarn.YarnJobContainer;
import ideal.sylph.runtime.yarn.YarnModule;
import ideal.sylph.spi.job.ContainerFactory;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobContainer;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.StreamingContext;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class SparkContainerFactory
        implements ContainerFactory
{
    private final IocFactory injector = IocFactory.create(new YarnModule(), binder -> {
        binder.bind(SparkAppLauncher.class).by(SparkAppLauncher.class).withSingle();
    });

    @Override
    public JobContainer createYarnContainer(Job job, String lastRunid)
    {
        SparkAppLauncher appLauncher = injector.getInstance(SparkAppLauncher.class);
        //----create JobContainer Proxy
        return new YarnJobContainer(appLauncher.getYarnClient(), lastRunid, () -> appLauncher.run(job));
    }

    @Override
    public JobContainer createLocalContainer(Job job, String lastRunid)
    {
        Supplier<?> jobHandle = (Supplier) job.getJobHandle();
        AtomicReference<String> url = new AtomicReference<>();
        JVMLauncher<Boolean> launcher = JVMLaunchers.<Boolean>newJvm()
                .setXms("512m")
                .setXmx("512m")
                .setCallable(() -> {
                    SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_local");
                    SparkContext sparkContext = new SparkContext(sparkConf);
                    Object appContext = requireNonNull(jobHandle, "sparkJobHandle is null").get();
                    if (appContext instanceof SparkSession) {
                        SparkSession sparkSession = (SparkSession) appContext;
                        checkArgument(sparkSession.streams().active().length > 0, "no stream pipeline");
                        sparkSession.streams().awaitAnyTermination();
                    }
                    else if (appContext instanceof StreamingContext) {
                        StreamingContext ssc = (StreamingContext) appContext;
                        ssc.start();
                        ssc.awaitTermination();
                    }
                    return true;
                })
                .setConsole(line -> {
                    String logo = "Bound SparkUI to 0.0.0.0, and started at";
                    if (url.get() == null && line.contains(logo)) {
                        url.set(line.split(logo)[1].trim());
                    }
                    System.out.println(line);
                })
                .notDepThisJvmClassPath()
                .addUserjars(job.getDepends())
                .build();

        return new LocalContainer()
        {
            @Override
            public String getJobUrl()
            {
                return url.get();
            }

            @Override
            public VmFuture startAsyncExecutor()
                    throws Exception
            {
                url.set(null);
                return launcher.startAsync();
            }
        };
    }
}
