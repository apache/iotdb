package org.apache.iotdb.db.concurrent;

import com.google.common.base.Throwables;

public abstract class WrappedRunnable implements Runnable
{
    public final void run()
    {
        try
        {
            runMayThrow();
        }
        catch (Exception e)
        {
            throw Throwables.propagate(e);
        }
    }
    abstract public void runMayThrow() throws Exception;
}
