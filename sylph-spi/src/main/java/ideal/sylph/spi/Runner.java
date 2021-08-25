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
package ideal.sylph.spi;

import com.github.harbby.gadtry.collection.ImmutableList;
import com.github.harbby.gadtry.spi.VolatileClassLoader;
import ideal.sylph.etl.Operator;
import ideal.sylph.spi.job.ContainerFactory;
import ideal.sylph.spi.job.JobEngineHandle;
import ideal.sylph.spi.model.OperatorInfo;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public interface Runner
{
    public void initialize(RunnerContext context)
            throws Exception;

    public Class<? extends ContainerFactory> getContainerFactory();

    public Set<JobEngineHandle> getEngines();

    public default List<OperatorInfo> getInternalOperator()
    {
        return ImmutableList.empty();
    }

    public default List<JobEngineHandle> analyzePlugin(Class<? extends Operator> operatorClass)
    {
        Set<JobEngineHandle> engines = this.getEngines();
        List<JobEngineHandle> supportEngines = new ArrayList<>(engines.size());
        for (JobEngineHandle engine : engines) {
            boolean isSupportOperator = Runner.analyzePlugin0(operatorClass, engine.keywords(), this.getClass());
            if (isSupportOperator) {
                supportEngines.add(engine);
            }
        }
        return supportEngines;
    }

    public static boolean analyzePlugin0(Class<? extends Operator> operatorClass,
            List<Class<?>> connectorGenerics,
            Class<? extends Runner> runnerClass)
    {
        //assert !RealTimePipeline.class.isAssignableFrom(operatorClass);
        //assert operatorClass.getGenericInterfaces().length > 0;
        VolatileClassLoader volatileClassLoader = (VolatileClassLoader) operatorClass.getClassLoader();
        volatileClassLoader.setSpiClassLoader(runnerClass.getClassLoader());
        try {
            Type[] types = operatorClass.getGenericInterfaces();
            boolean isSupportOperator = connectorGenerics.contains(
                    ((ParameterizedType) ((ParameterizedType) types[0]).getActualTypeArguments()[0]).getRawType()
            );
            return isSupportOperator;
        }
        catch (TypeNotPresentException e) {
            return false;
        }
    }
}
