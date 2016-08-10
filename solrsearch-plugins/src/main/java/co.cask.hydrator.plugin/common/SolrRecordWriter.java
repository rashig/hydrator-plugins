/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.hydrator.plugin.common;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.format.StructuredRecordStringConverter;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * SolrRecordWriter - Instantiate a record writer that will build a Solr index.
 */
public class SolrRecordWriter extends RecordWriter<Text, Text> {
  private static final String SERVER_URL = "solr.server.url";
  private static final String SERVER_MODE = "solr.server.mode";
  private static final String COLLECTION_NAME = "solr.server.collection";
  private static final String KEY_FIELD = "solr.server.keyfield";
  private static final String FIELD_MAPPINGS = "solr.output.field.mappings";
  private static final String BATCH_SIZE = "solr.batch.size";
  private static final Gson GSON = new Gson();
  private final SolrSearchSinkConfig config;
  List<SolrInputDocument> documentList = new ArrayList<SolrInputDocument>();
  private SolrClient solrClient;
  private Configuration conf;

  public SolrRecordWriter(TaskAttemptContext context) {
    conf = context.getConfiguration();
    config = new SolrSearchSinkConfig(null, conf.get(SERVER_MODE), conf.get(SERVER_URL), conf.get(COLLECTION_NAME),
                                      conf.get(KEY_FIELD), conf.get(FIELD_MAPPINGS));
    solrClient = config.getSolrConnection();
  }

  @Override
  public void write(Text key, Text value) throws IOException {
    String solrFieldName;
    SolrInputDocument document;
    Type schemaType = new TypeToken<Schema>() { }.getType();

    Schema inputSchema = GSON.fromJson(key.toString(), schemaType);
    StructuredRecord structuredRecord = StructuredRecordStringConverter.fromJsonString(value.toString(), inputSchema);
    document = new SolrInputDocument();
    for (Schema.Field field : structuredRecord.getSchema().getFields()) {
      solrFieldName = field.getName();
      if (config.getOutputFieldMap().containsKey(solrFieldName)) {
        document.addField(config.getOutputFieldMap().get(solrFieldName), structuredRecord.get(solrFieldName));
      } else {
        document.addField(solrFieldName, structuredRecord.get(solrFieldName));
      }
    }
    documentList.add(document);
    try {
      if (documentList.size() == Integer.parseInt(conf.get(BATCH_SIZE))) {
        solrClient.add(documentList);
        solrClient.commit();
        documentList.clear();
      }
    } catch (SolrServerException e) {
      throw new IllegalArgumentException("Exception while indexing the documents to Solr. For more details, Please " +
                                           "check the logs.", e);
    }
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    try {
      if (!documentList.isEmpty()) {
        solrClient.add(documentList);
        solrClient.commit();
      }
    } catch (SolrServerException e) {
      throw new IllegalArgumentException("Exception while indexing the documents to Solr. For more details, Please " +
                                           "check the logs.", e);
    } finally {
      documentList.clear();
    }
    solrClient.shutdown();
  }
}
