package org.scut.ccnl.genomics;

import org.broadinstitute.gatk.utils.activeregion.ActiveRegion;
import org.broadinstitute.gatk.utils.refdata.RefMetaDataTracker;

import java.io.Serializable;

/**
 * Data to use in the ActiveRegionWalker.map function produced by the NanoScheduler input iterator
 */
public class MapData implements Serializable {
    public ActiveRegion activeRegion;
    public RefMetaDataTracker tracker;

    public MapData(ActiveRegion activeRegion, RefMetaDataTracker tracker) {
        this.activeRegion = activeRegion;
        this.tracker = tracker;
    }
}