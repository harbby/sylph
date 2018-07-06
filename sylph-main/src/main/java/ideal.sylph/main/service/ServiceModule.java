package ideal.sylph.main.service;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

public final class ServiceModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(BatchJobManager.class).in(Scopes.SINGLETON);
        binder.bind(StreamJobManager.class).in(Scopes.SINGLETON);
    }
}
