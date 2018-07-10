package ideal.sylph.spi.utils;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.text.DateFormat;
import java.util.Date;

public class DirClassLoader
        extends URLClassLoader
{
    private final Date startDate = new Date();

    public DirClassLoader()
    {
        super(new URL[0]);
    }

    /**
     * 构造函数
     */
    public DirClassLoader(ClassLoader parent)
    {
        super(new URL[0], parent);
    }

    @Override
    public void close()
            throws IOException
    {
        // TODO Auto-generated method stub
        super.close();
    }

    /**
     * Adds a jar file from the filesystems into the jar loader list.
     *
     * @param jarfile The full path to the jar file.
     * @throws MalformedURLException
     */
    public void addJarFile(String jarfile)
            throws MalformedURLException
    {
        URL url = new URL("file:" + jarfile);
        this.addURL(url);
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

    public void addJarFile(File jarfile)
            throws MalformedURLException
    {
        this.addURL(jarfile.toURI().toURL());
    }

    public void addDir(String path)
            throws MalformedURLException
    {
        addDir(new File(path));
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
        // TODO Auto-generated method stub
        return super.toString() + ",time:" + DateFormat.getTimeInstance().format(startDate);
    }
}
