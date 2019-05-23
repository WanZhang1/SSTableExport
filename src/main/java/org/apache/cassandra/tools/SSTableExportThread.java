/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.tools;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletedColumn;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.cassandra.utils.ByteBufferUtil.bytesToHex;

/**
 * Export SSTables to JSON format.
 */
public class SSTableExportThread {
    private static ObjectMapper jsonMapper = new ObjectMapper();

    private static final String KEY_OPTION = "k";
    private static final String EXCLUDEKEY_OPTION = "x";
    private static final String ENUMERATEKEYS_OPTION = "e";

    private static Options options;
    private static CommandLine cmd;

    static {
        options = new Options();

        Option optKey = new Option(KEY_OPTION, true, "Row key");
        // Number of times -k <key> can be passed on the command line.
        optKey.setArgs(500);
        options.addOption(optKey);

        Option excludeKey = new Option(EXCLUDEKEY_OPTION, true, "Excluded row key");
        // Number of times -x <key> can be passed on the command line.
        excludeKey.setArgs(500);
        options.addOption(excludeKey);

        Option optEnumerate = new Option(ENUMERATEKEYS_OPTION, false, "enumerate keys only");
        options.addOption(optEnumerate);

        // disabling auto close of the stream
        jsonMapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
    }

    /**
     * Serialize columns using given column iterator
     *
     * @param columns column iterator
     * @param out output stream
     * @param comparator columns comparator
     * @param cfMetaData Column Family metadata (to get validator)
     */
    private static void serializeColumns(Iterator<IColumn> columns, FSDataOutputStream out, AbstractType<?> comparator, CFMetaData cfMetaData) throws IOException{
        while (columns.hasNext()) {
            out.write((serializeColumn(columns.next(), comparator, cfMetaData)).getBytes());
            if (columns.hasNext())
                out.write(",".getBytes());
        }
    }

    /**
     * Serialize a given column to the JSON format
     *
     * @param column column presentation
     * @param comparator columns comparator
     * @param cfMetaData Column Family metadata (to get validator)
     *
     * @return column as serialized list
     */
    private static String serializeColumn(IColumn column, AbstractType<?> comparator, CFMetaData cfMetaData)
    {
        StringBuffer serializedColumn = new StringBuffer();

        ByteBuffer name = ByteBufferUtil.clone(column.name());
        ByteBuffer value = ByteBufferUtil.clone(column.value());

        if (column instanceof DeletedColumn) {
            serializedColumn.append(ByteBufferUtil.bytesToHex(value));
        } else {
            AbstractType<?> validator = cfMetaData.getValueValidator(name);
            serializedColumn.append(validator.getString(value));
        }
        return serializedColumn.toString();
    }

    /**
     * Get portion of the columns and serialize in loop while not more columns left in the row
     * @param row SSTableIdentityIterator row representation with Column Family
     * @param key Decorated Key for the required row
     * @param out output stream
     */
    private static void serializeRow(SSTableIdentityIterator row, DecoratedKey key, FSDataOutputStream out)
            throws IOException{
        ColumnFamily columnFamily = row.getColumnFamily();
        boolean isSuperCF = columnFamily.isSuper();
        CFMetaData cfMetaData = columnFamily.metadata();
        AbstractType<?> comparator = columnFamily.getComparator();
        out.write((new String(key.key.array())+",").getBytes());
        if (isSuperCF) {
            while (row.hasNext()) {
                IColumn column = row.next();
                //out.print(", ");
                serializeColumns(column.getSubColumns().iterator(), out, columnFamily.getSubComparator(), cfMetaData);
            }
        } else {
            serializeColumns(row, out, comparator, cfMetaData);
        }
    }



    /**
     * Given arguments specifying an SSTable, and optionally an output file,
     * export the contents of the SSTable to JSON.
     *
     * @param args command lines arguments
     *
     * @throws IOException on failure to open/read/write files or output streams
     * @throws ConfigurationException on configuration failure (wrong params given)
     */
    public static void main(String[] args) throws IOException, ConfigurationException {
        String usage = String.format("Usage: %s sstable_dir hdfs_dir ,eg:" +
                "/var/lib/cassandra/data/user_train/sale_record/  hdfs://192.168.3.8:8020/test/ " +
                "%n", SSTableExportThread.class.getName());
        CommandLineParser parser = new PosixParser();
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e1) {
            System.err.println(e1.getMessage());
            System.err.println(usage);
            System.exit(1);
        }
        if (cmd.getArgs().length != 2) {
            System.err.println("You must supply one sstable dir and one hdfs dir");
            System.err.println(usage);
            System.exit(1);
        }
        DatabaseDescriptor.loadSchemas();// 加载schema

        String[] excludes = cmd.getOptionValues(EXCLUDEKEY_OPTION);
        String hdfsFilePath = cmd.getArgs()[1];
        File file = new File(cmd.getArgs()[0]);
        File[] fs = file.listFiles();

        ExecutorService pool = Executors.newFixedThreadPool(10);
        for(File f:fs){
            if(!f.isDirectory() && f.getName().endsWith("-Data.db")){
                String ssTableFilePath = f.getAbsolutePath();

                if (Schema.instance.getNonSystemTables().size() < 1) {
                    String msg = "no non-system tables are defined";
                    System.err.println(msg);
                    throw new ConfigurationException(msg);
                }
                Descriptor descriptor = Descriptor.fromFilename(ssTableFilePath);
                if (Schema.instance.getCFMetaData(descriptor) == null) {
                    System.err.println(String.format("The provided column family is not part of this cassandra database: keysapce = %s, column family = %s", descriptor.ksname, descriptor.cfname));
                }else{
                    //export(descriptor, out,excludes);
                    String hdfsFileName=hdfsFilePath+f.getName().replace(".db","");

                    pool.execute(new WriteToHdfs(descriptor, hdfsFileName,excludes));
                }
            }
        }
        // 关闭线程池
        pool.shutdown();
        System.exit(0);
    }

   static  class WriteToHdfs implements Runnable{
        Descriptor descriptor;
        String hdfsFileName;
        String[] excludes;
        public WriteToHdfs(Descriptor descriptor,String hdfsFileName,String[] excludes){
            this.descriptor=descriptor;
            this.hdfsFileName=hdfsFileName;
            this.excludes=excludes;

        }
        public void run() {
            try{
                FSDataOutputStream out = getOutputStream(hdfsFileName);
                System.out.println("begin to write:"+hdfsFileName);

                export(SSTableReader.open(descriptor), out, excludes);

                System.out.println("===:"+hdfsFileName);


            }catch (IOException e){
                e.printStackTrace();
            }
        }

       // This is necessary to accommodate the test suite since you cannot open a Reader more
       // than once from within the same process.
       public   void export(SSTableReader reader, FSDataOutputStream outs, String[] excludes) throws IOException {
           System.out.println("----export---");

           Set<String> excludeSet = new HashSet<String>();

           if (excludes != null)
               excludeSet = new HashSet<String>(Arrays.asList(excludes));

           SSTableIdentityIterator row;
           SSTableScanner scanner = reader.getDirectScanner();
           int i = 0;
           // collecting keys to export
           while (scanner.hasNext()) {
               row = (SSTableIdentityIterator) scanner.next();
               String currentKey = bytesToHex(row.getKey().key);
               if (excludeSet.contains(currentKey))
                   continue;
               else if (i != 0)
                   outs.write("\n".getBytes());
               serializeRow(row, row.getKey(), outs);
               i++;
           }
           outs.flush();
           outs.close();
           scanner.close();
       }

       public   FSDataOutputStream getOutputStream(String toUri) throws IOException {
            System.out.println("new out:"+toUri);
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(toUri), conf);
            FSDataOutputStream out = fs.create(new Path(toUri));
            return out;
        }
    }




}
