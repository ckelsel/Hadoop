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
package com.ckelsel.hadoop.dfs.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
 
import java.io.InputStream;
import java.net.URI;
 
public class Test {
    public static void main(String[] args) throws Exception {
        String uri = "hdfs://localhost:9000/";
        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), config);
 
        // 列出hdfs上/user/ckelsel/目录下的所有文件和目录
        FileStatus[] statuses = fs.listStatus(new Path("/user/ckelsel"));
        for (FileStatus status : statuses) {
            System.out.println(status);
        }
 
        // 在hdfs的/user/ckelsel目录下创建一个文件，并写入一行文本
        FSDataOutputStream os = fs.create(new Path("/user/ckelsel/test.log"));
        os.write("Hello World!".getBytes());
        os.flush();
        os.close();
 
        // 显示在hdfs的/user/ckelsel下指定文件的内容
        InputStream is = fs.open(new Path("/user/ckelsel/test.log"));
        IOUtils.copyBytes(is, System.out, 1024, true);
    }
}