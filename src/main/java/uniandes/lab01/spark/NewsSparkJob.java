/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniandes.lab01.spark;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 *
 * @author teo
 */
public class NewsSparkJob implements Serializable{
    
    private final static String TITLE_REGEX="<TITLE>(.*)</TITLE>";
    private static Pattern titlePattern= Pattern.compile(TITLE_REGEX);
    
    private final static String BODY_REGEX01="<BODY>(.*)(</BODY>)?";
    private static Pattern boddyPattern01= Pattern.compile(BODY_REGEX01);
    
    private final static String BODY_REGEX02="(^<)|(>$)|(^&#)";
    private static Pattern boddyPattern02= Pattern.compile(BODY_REGEX02);
    
    private final static String BODY_REGEX03="(.*)</BODY>";
    private static Pattern boddyPattern03= Pattern.compile(BODY_REGEX03);
    
    private String title_key = null;

    public NewsSparkJob(String entrada, String salida) {
        SparkConf conf = new SparkConf().setAppName("NewsSparkJob");
        //conf.setMaster("local[*]");//Descomentar esta línea para probar localmente	

        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaRDD<String> texto = sc.textFile(entrada);
        
        ArrayList<Tuple2<String, String>> list = new ArrayList<>();
        
        JavaPairRDD<String, Integer> pairs =texto.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String t) throws Exception {
                String originalLine= t;
                String line = StringEscapeUtils.unescapeHtml(originalLine);
                Matcher m = titlePattern.matcher(line);
                if (m.find()) {
                    String title = m.group(1);
                    String words[] = title.split("([().,!?:;'\"-]|\\s)+");
                    if (words.length > 5) {
                        title_key = title;
                    } else {
                        title_key = null;
                    }
                } else {
                    if (title_key != null) {
                        m = boddyPattern01.matcher(line);
                        if (m.find()) {
                            String body_part = m.group(1);
                            String words[] = body_part.split("([().,!?:;'\"-]|\\s)+");
                            return new Tuple2<String, Integer>(title_key, new Integer(words.length));
                        } else {
                            m = boddyPattern03.matcher(line);
                            if (m.find()) {
                                String body_part = m.group(1);
                                String words[] = body_part.split("([().,!?:;'\"-]|\\s)+");
                                return new Tuple2<String, Integer>(title_key, new Integer(words.length));
                            } else {
                                m = boddyPattern02.matcher(originalLine);
                                if (!m.find()) {
                                    String body_part = line;
                                    String words[] = body_part.split("([().,!?:;'\"-]|\\s)+");
                                    return new Tuple2<String, Integer>(title_key, new Integer(words.length));                                    
                                }
                            }
                        }
                    }
                }
                return new Tuple2<String, Integer>("",  new Integer(0));
            }
            
        });
        
        JavaPairRDD<String, Integer> byKey = pairs.reduceByKey(
                new Function2<Integer, Integer, Integer>() {

            public Integer call(Integer t1, Integer t2) throws Exception {
                return t1+t2;
            }
        });
        byKey.saveAsTextFile(salida);;
    }
    
    public static void main(String[] args) {
        String entrada = null;
        String salida = null;
        /**
         * Recibir como parámetro el nombre del archivo
		 *
         */
        if (args.length >= 2) {
            entrada = args[0];
            salida = args[1];
        } else {
            System.err.println("No se puede ejecutar sin especificar entrada y salida");
            return;
        }
        new NewsSparkJob(entrada, salida);

    }
}
