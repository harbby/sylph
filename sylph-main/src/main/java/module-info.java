/**
 * Created by ideal on 18-6-29.
 */

module ideal.sylph.main {

    //requires java.sql;

    requires com.google.guice;
    requires com.google.common;
    requires org.slf4j;

    requires ideal.sylph.spi;
    requires log4j;

    exports ideal.sylph.main.bootstrap;
    exports ideal.sylph.main.service;
//    exports com.ideal.java9.mod1;
//    exports com.ideal.java9.utils;
}