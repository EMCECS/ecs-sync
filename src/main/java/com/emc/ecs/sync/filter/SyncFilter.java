package com.emc.ecs.sync.filter;

import com.emc.ecs.sync.SyncPlugin;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.SyncObject;

public interface SyncFilter<C> extends SyncPlugin<C> {
    /**
     * Implement your main logic for the filter here.  To pass the object to
     * the next filter in the chain, you must call
     * <code>getNext().filter(obj)</code>.  However, if you decide that you
     * do not wish to send the object down the chain, you
     * can simply return.  Once the above method returns, your object has
     * completed its journey down the chain and has "come back".  This is the
     * point where you can implement any post-processing logic.
     * @param objectContext the context of the current sync task to inspect and/or modify.
     */
    void filter(ObjectContext objectContext);

    /**
     * Override to <em>remove</em> any transformations your filter has made on the object.
     * You must obtain the object through a call to <code>getNext().reverseFilter()</code>
     * and assume that a new object is returned that was pulled from the target system.
     * This object must also be returned from your implementation after any necessary
     * (reversal) modifications. Plugins that do not modify the object may simply return
     * <code>getNext().reverseFilter()</code>.
     */
    SyncObject reverseFilter(ObjectContext objectContext);

    /**
     * Returns the next filter in the chain, creating a linked-list.
     * @return the next filter
     */
    SyncFilter getNext();


    /**
     * Sets the next filter in the chain.
     * @param next the next filter to set
     */
    void setNext(SyncFilter next);
}
