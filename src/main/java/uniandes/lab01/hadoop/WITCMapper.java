/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniandes.lab01.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringEscapeUtils;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WITCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    private final static String TITLE_REGEX="<TITLE>(.*)</TITLE>";
    private static Pattern titlePattern= Pattern.compile(TITLE_REGEX);
    
    private final static String BODY_REGEX01="<BODY>(.*)(</BODY>)?";
    private static Pattern boddyPattern01= Pattern.compile(BODY_REGEX01);
    
    private final static String BODY_REGEX02="(^<)|(>$)|(^&#)";
    private static Pattern boddyPattern02= Pattern.compile(BODY_REGEX02);
    
    private final static String BODY_REGEX03="(.*)</BODY>";
    private static Pattern boddyPattern03= Pattern.compile(BODY_REGEX03);
    
    private static String title_key=null;

    @Override
    protected void map(LongWritable key, Text value,
            Context context)
            throws IOException, InterruptedException {
        // Now create matcher object.
        String originalLine= value.toString();
        String line = StringEscapeUtils.unescapeHtml(originalLine);
        Matcher m = titlePattern.matcher(line);
        if (m.find()) {
            String title= m.group(1);
            String words[]= title.split("([().,!?:;'\"-]|\\s)+");            
            if(words.length > 5){                                               
                title_key = title;
                
            }else{
                title_key=null;
            }
        }else{
            if (title_key!=null){
                m = boddyPattern01.matcher(line);
                if (m.find()){
                    String body_part= m.group(1);
                    String words[]= body_part.split("([().,!?:;'\"-]|\\s)+");
                    context.write(new Text(title_key), new IntWritable(words.length));                
                }else {
                    m = boddyPattern03.matcher(line);
                    if (m.find()) {
                        String body_part = m.group(1);
                        String words[]= body_part.split("([().,!?:;'\"-]|\\s)+");
                        context.write(new Text(title_key), new IntWritable(words.length));
                    }else{
                        m = boddyPattern02.matcher(originalLine);
                        if (!m.find()) {
                            String body_part= line;
                            String words[]= body_part.split("([().,!?:;'\"-]|\\s)+");
                            context.write(new Text(title_key), new IntWritable(words.length));
                        }
                    }
                }
            }
        }
    }
}
