package ideal.sylph.spi.classloader;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLStreamHandlerFactory;
import java.text.DateFormat;
import java.util.Collection;
import java.util.Date;

import static org.apache.commons.lang3.ObjectUtils.firstNonNull;

public class DirClassLoader
        extends URLClassLoader
{
    private final Date startDate = new Date();

    public DirClassLoader(URL[] urls, ClassLoader parent)
    {
        super(firstNonNull(urls, new URL[0]), parent);
    }

    public DirClassLoader(URL[] urls)
    {
        super(urls);
    }

    public DirClassLoader(URL[] urls, ClassLoader parent, URLStreamHandlerFactory factory)
    {
        super(urls, parent, factory);
    }

    /**
     * Adds a jar file from the filesystems into the jar loader list.
     *
     * @param jarfile The full path to the jar file.
     */
    public void addJarFile(URL jarfile)
    {
        this.addURL(jarfile);
    }

    public void addJarFile(Collection<URL> jarfiles)
    {
        jarfiles.forEach(this::addJarFile);
    }

    public void addJarFile(File jarfile)
            throws MalformedURLException
    {
        this.addURL(jarfile.toURI().toURL());
    }

    public void addDir(File path)
            throws MalformedURLException
    {
        if (!path.exists()) {
            return;
        }

        if (path.isDirectory()) {
            File[] files = path.listFiles();
            if (files != null) {
                for (File file : files) {
                    this.addDir(file);
                }
            }
        }
        else { //文件
            this.addJarFile(path);
        }
    }

    @Override
    public String toString()
    {
        return super.toString() + ",time:" + DateFormat.getTimeInstance().format(startDate);
    }
}
