package com.emc.vipr.sync.filter;

import com.emc.vipr.sync.model.SyncObject;
import com.emc.vipr.sync.SyncPlugin;

public abstract class SyncFilter extends SyncPlugin {
    private SyncFilter next;

    /**
     * Implement this method to return the --filter parameter value which will activate this filter
     * and add it to the filter chain.  The activation name should including only lowercase letters, numbers and dashes.
     * I.e. if you return "my-filter", then the CLI argument "--filter my-filter" will
     * activate this plugin and insert it into the chain at its corresponding place in the --filter options.  Multiple
     * filters are specified as "--filter filter1 --filter filter2 --filter filter3 ..."
     * @return the "--filter" parameter value which will activate this filter.
     */
    public abstract String getActivationName();

    /**
     * Implement your main logic for the filter here.  To pass the object to
     * the next filter in the chain, you must call
     * <code>getNext().filter(obj)</code>.  However, if you decide that you
     * do not wish to send the object down the chain, you
     * can simply return.  Once the above method returns, your object has
     * completed its journey down the chain and has "come back".  This is the
     * point where you can implement any post-processing logic.
     * @param obj the SyncObject to inspect and/or modify.
     */
    public abstract void filter(SyncObject<?> obj);

    /**
     * Returns the next filter in the chain, creating a linked-list.
     * @return the next filter
     */
    public SyncFilter getNext() {
        return next;
    }

    /**
     * Sets the next filter in the chain.
     * @param next the next filter to set
     */
    public void setNext(SyncFilter next) {
        this.next = next;
    }
}
