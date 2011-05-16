package org.sleuthkit.hadoop;

import java.util.regex.Pattern;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public abstract class SKMapper<keyin, valin, keyout, valout>
extends Mapper<keyin, valin, keyout, valout> {

    private String id;
    
    
    @Override
    public void setup(Context ctx) {
        id = ctx.getConfiguration().get("org.sleuthkit.hadoop.imageid", "DEFAULT_IMAGE_ID");
        System.out.println(id);
    }
    

    String getId() {
        return id;
    }
    
}