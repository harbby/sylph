package ideal.sylph.controller;

import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;

//@javax.ws.rs.ApplicationPath(ResourcePath.API_ROOT)
public class WebApplication
        extends ResourceConfig
{
    public WebApplication()
    {
        //The jackson feature and provider is used for object serialization
        //between client and server objects in to a json
//        register(JacksonFeature.class);
//        register(JacksonProvider.class);

        register(AppExceptionMapper.class);
        //Glassfish multipart file uploader feature
        register(MultiPartFeature.class);
        packages("ideal.sylph.controller.action");
    }

    //servlet.setInitParameter("jersey.config.server.provider.packages","ideal.sylph.controller.action");
}
