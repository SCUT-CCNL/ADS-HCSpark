package org.scut.ccnl.genomics;

import com.esotericsoftware.kryo.Kryo;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.variant.variantcontext.*;
import org.apache.spark.serializer.KryoRegistrator;
import org.broadinstitute.gatk.utils.GenomeLoc;
import org.broadinstitute.gatk.utils.GenomeLocParser;
import org.broadinstitute.gatk.utils.activeregion.ActivityProfileState;
import org.scut.ccnl.genomics.io.CollectionsNCopiesSerializer;

import java.util.Collections;


public class GATKRegistrator implements KryoRegistrator {

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public void registerClasses(Kryo kryo) {

        // htsjdk.variant.variantcontext.CommonInfo has a Map<String, Object> that defaults to
        // a Collections.unmodifiableMap. This can't be handled by the version of kryo used in Spark, it's fixed
        // in newer versions (3.0.x), but we can't use those because of incompatibility with Spark. We just include the
        // fix here.
        // We are tracking this issue with (#874)
        kryo.register(Collections.unmodifiableMap(Collections.EMPTY_MAP).getClass(), new UnmodifiableCollectionsSerializer());
        kryo.register(Collections.unmodifiableList(Collections.EMPTY_LIST).getClass(), new UnmodifiableCollectionsSerializer());
        kryo.register(Collections.nCopies(1, new Object()).getClass(),
                new CollectionsNCopiesSerializer());

        kryo.register(CommonInfo.class);
        kryo.register(Allele.class);
        kryo.register(GenotypesContext.class);
        kryo.register(VariantContext.class);
        kryo.register(SAMSequenceDictionary.class);
        kryo.register(SAMSequenceRecord.class);
        kryo.register(GenomeLocParser.class);
        kryo.register(GlobalArgument.class);
        kryo.register(ActivityProfileState.class);
        kryo.register(GenomeLoc.class);
        kryo.register(FastGenotype.class);
        kryo.register(Genotype.class);

    }
}