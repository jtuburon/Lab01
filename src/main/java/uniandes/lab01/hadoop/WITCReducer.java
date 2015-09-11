/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniandes.lab01.hadoop;

import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author teo
 */
import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WITCReducer extends Reducer<Text, Text, Text, IntWritable> {
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {
            Iterator<Text> i = values.iterator();
            int counter=0;
            String textv="";
            for(Text t : values) {
                textv = t.toString();
                String words[]= textv.split("([().,!?:;'\"-]|\\s)+");  
                counter = counter + words.length;
            }
            context.write(new Text(key), new IntWritable(counter));
	}

}
