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

import com.github.harbby.gadtry.base.Lazys;
import com.github.harbby.gadtry.ioc.IocFactory;
import com.github.harbby.gadtry.jvm.JVMLaunchers;
import ideal.sylph.runner.spark.yarn.SparkAppLauncher;
import ideal.sylph.runtime.local.LocalContainer;
import ideal.sylph.runtime.yarn.YarnJobContainer;
import ideal.sylph.runtime.yarn.YarnModule;
import ideal.sylph.spi.App;
import ideal.sylph.spi.job.ContainerFactory;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobContainer;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.StreamingContext;

import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class SparkContainerFactory
        implements ContainerFactory
{
    private final Supplier<SparkAppLauncher> yarnLauncher = Lazys.goLazy(() -> {
        IocFactory injector = IocFactory.create(new YarnModule());
        return injector.getInstance(SparkAppLauncher.class);
    });

    @Override
    public JobContainer getYarnContainer(Job job, String lastRunid)
    {
        SparkAppLauncher appLauncher = yarnLauncher.get();
        //----create JobContainer Proxy
        return YarnJobContainer.of(appLauncher.getYarnClient(), lastRunid, () -> appLauncher.run(job));
    }

    @Override
    public JobContainer getLocalContainer(Job job, String lastRunid)
    {
        SparkJobHandle<App<?>> jobHandle = (SparkJobHandle) job.getJobHandle();

        JVMLaunchers.VmBuilder<Boolean> vmBuilder = JVMLaunchers.<Boolean>newJvm()
                .setCallable(() -> {
                    SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_local");
                    SparkContext sparkContext = new SparkContext(sparkConf);
                    App<?> app = requireNonNull(jobHandle, "sparkJobHandle is null").getApp().get();
                    app.build();
                    Object appContext = app.getContext();
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
                .setXms("512m")
                .setXmx("512m")
                .setConsole(System.out::println)
                .notDepThisJvmClassPath()
                .addUserjars(job.getDepends());

        return new LocalContainer(vmBuilder);
    }

    @Override
    public JobContainer getK8sContainer(Job job, String lastRunid)
    {
        throw new UnsupportedOperationException("this method have't support!");
    }
}
