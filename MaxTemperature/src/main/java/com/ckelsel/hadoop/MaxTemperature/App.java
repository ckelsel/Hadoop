/**-------------------------------------------------------------------------
 * Copyright (C) 2015 KunMing Xie <kunming.xie@hotmail.com>
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

package com.ckelsel.hadoop.MaxTemperature;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class App {
    public static void main( String[] args ) {
        if (args.length != 2) {
        	System.err.println("Usage: MaxTemperature <input path> <output path>");
            System.exit(-1);
        }
        
        System.out.println(args[0]);
        System.out.println(args[1]);
        
        try {
        	Configuration conf = new Configuration();
        	conf.set("mapred.job.tracker", "localhost:9001");
        	
			Job job = Job.getInstance(conf);
			
			job.setJarByClass(App.class);
			job.setJobName("Max temperature");
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			
			// delete output if exists
			Path outPath = new Path(args[1]);
			outPath.getFileSystem(conf).delete(outPath, true);
			
			FileOutputFormat.setOutputPath(job, outPath);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			System.exit(job.waitForCompletion(true) ? 0 : -1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}
