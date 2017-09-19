package agapes.agapes_pack;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;


public class App 
{	
	
    public static void main( String[] args ) throws FileNotFoundException, IOException, InterruptedException, TimeoutException
    {
    	Storage storage = StorageOptions.getDefaultInstance().getService();
    	BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    	Bucket bucket = storage.get("agapes_storage");
    	Blob blob = bucket.get("Tickets/K113514051.txt");
    	
    	
    	HashMap<String, String> type = new HashMap<String, String>();
    	
    	if (blob != null) {
    		  byte[] prevContent = blob.getContent();
    		  String content = new String(prevContent, UTF_8);
    		  String []content_line = content.split("\n");
    		  String date = null; 
    		  String restaurent = null; 
    		  
    		  String datasetName = "Agapes_Datasets";
    		
    		  // definition des attributs
    		  Field field = Field.of("Ligne", Field.Type.string());
    		  Field field1 = Field.of("Date", Field.Type.string());
    		  Field field2 = Field.of("Restaurent", Field.Type.string());
    		  Field field3 = Field.of("TPV", Field.Type.string());
    		  Field field4 = Field.of("Tiroir", Field.Type.string());
    		  Field field5 = Field.of("Ticket", Field.Type.string());
    		  Field field6 = Field.of("Emp", Field.Type.string());
    		  Field field7 = Field.of("Ligne", Field.Type.string());
    		  Field field8 = Field.of("Heure", Field.Type.string());
    		  Field field9 = Field.of("Type", Field.Type.string());
    		  Field field10 = Field.of("Ligne_", Field.Type.string());
    		  Field field11 = Field.of("PLU", Field.Type.string());
    		  Field field12 = Field.of("PLU_code", Field.Type.string());
    		  Field field13 = Field.of("Montant", Field.Type.string());
    		  


    		  restaurent = content_line[2].substring(content_line[2].indexOf("Restaurant:")+ "Restaurant:".length(),content_line[2].indexOf("Restaurant:")+ "Restaurant:".length()+4);
			  date = content_line[2].substring(content_line[2].indexOf("Date: ")+ "Date: ".length(),content_line[2].indexOf("Date: ")+ "Date: ".length()+10);
			  type.put("ACT", "");
			  type.put("DAT", "");
			  type.put("PMN", "");
			  type.put("PLU", "");
			  type.put("NPR", "");
			  type.put("REP", "");
			  type.put("RGT", "");
			  type.put("DMN", "");
			  type.put("TOT", "");
			  type.put("TVA", "");
			  type.put("FID", "");
			  type.put("RECAP", "");
    		  for(int i = 0; i<content_line.length;i++) {
    			  if (content_line[i].contains(" ACT ")) {
    				  //ACT = ACT + "\n" + date + " " + restaurent + content_line[i];
    				  type.replace("ACT",type.get("ACT").concat("\n" + date + " " + restaurent + content_line[i]));
    			  }
    			  if (content_line[i].contains(" DAT ")) {
    				  type.replace("DAT",type.get("DAT").concat("\n" + date + " " + restaurent + content_line[i]));
    			  }
    			  
    			  if (content_line[i].contains(" PMN ")) {
    				  type.replace("PMN",type.get("PMN").concat("\n" + date + " " + restaurent + content_line[i]));
    			  }
    			  if (content_line[i].contains(" PLU ")) {
    				  type.replace("PLU",type.get("PLU").concat("\n" + date + " " + restaurent + content_line[i]));
    			  }
    			  if (content_line[i].contains(" NPR ")) {
    				  //ACT = ACT + "\n" + date + " " + restaurent + content_line[i];
    				  type.replace("NPR",type.get("NPR").concat("\n" + date + " " + restaurent + content_line[i]));
    			  }
    			  if (content_line[i].contains(" REP ")) {
    				  type.replace("REP",type.get("REP").concat("\n" + date + " " + restaurent + content_line[i]));
    			  }
    			  
    			  if (content_line[i].contains(" RGT ")) {
    				  type.replace("RGT",type.get("RGT").concat("\n" + date + " " + restaurent + content_line[i]));
    			  }
    			  if (content_line[i].contains(" DMN ")) {
    				  type.replace("DMN",type.get("DMN").concat("\n" + date + " " + restaurent + content_line[i]));
    			  }
    			  
    			  if (content_line[i].contains(" TOT ")) {
    				  type.replace("TOT",type.get("TOT").concat("\n" + date + " " + restaurent + content_line[i]));
    			  }
    			  
    			  if (content_line[i].contains(" TVA ")) {
    				  type.replace("TVA",type.get("TVA").concat("\n" + date + " " + restaurent + content_line[i]));
    			  }
    			  if (content_line[i].contains(" FID ")) {
    				  type.replace("FID",type.get("FID").concat("\n" + date + " " + restaurent + content_line[i]));
    			  }
    			  else
    			  {
    				  type.replace("RECAP",type.get("RECAP").concat("\n" + content_line[i]));
    			  }
    		  }
    	
    		  for(String key : type.keySet()) {
    			  
    			  BlobId blobId = BlobId.of("agapes_storage", "Tickets/"+key+".txt");
        		  BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
        		  Blob blob_act = storage.create(blobInfo, type.get(key).toString().getBytes(UTF_8));
        		 
        		  ////// table temporaire/////// 
        		  
        		  TableId tableId = TableId.of(datasetName, key+"_temp");
        		  
        		  // Table schema definition
        		  Schema schema = Schema.of(field);
        		  TableDefinition tableDefinition = StandardTableDefinition.of(schema);
        		  TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        		  Table table = bigquery.create(tableInfo);
        		  table.load(FormatOptions.csv(), "gs://agapes_storage/Tickets/"+key+".txt"); 
    			  
    		  }
    		  TableId tableId1 = TableId.of(datasetName, "bigtable");
    		  
    		  Schema schema1 = Schema.of(field1,field2,field3,field4,field5,field6,field7,field8,field9,field10,field11,field12,field13);
    		  
    		  TableDefinition tableDefinition1 = StandardTableDefinition.of(schema1);
    		  TableInfo tableInfo1 = TableInfo.newBuilder(tableId1, tableDefinition1).build();
    		  Table table1 = bigquery.create(tableInfo1);
    		 
    		  for ( String key : type.keySet() ) {
    		  if(key.equals("ACT")||key.equals("DAT")||key.equals("NPR")||key.equals("REP")||key.equals("FID")) {
    		  /////table final ////
    		  
    		  QueryJobConfiguration queryConfig =
    				    QueryJobConfiguration.newBuilder("insert into Agapes_Datasets.bigtable (Date,Restaurent,TPV,Tiroir,Ticket,Emp,Ligne,Heure,Type,Ligne_) select substr(Ligne, 1,11) as Date, substr(Ligne,12,4) as Restaurent, \r\n" + 
    				    		"       substr(Ligne,18,2) as TPV,substr(Ligne, 24,3) as Tiroir,\r\n" + 
    				    		"       substr(Ligne, 29,5) as Ticket, substr(Ligne, 35,3) as Emp, substr(Ligne, 39,5) as Ligne, \r\n" + 
    				    		"       substr(Ligne, 45,6) as Heure, substr(Ligne, 52,3) as Type, substr(Ligne, 56) as Ligne_ from Agapes_Datasets."+key+"_temp")
    				        .setUseLegacySql(false)
    				        .build();
    		  
    		  JobId jobId = JobId.of(UUID.randomUUID().toString());
    		  com.google.cloud.bigquery.Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

    		  
    		  queryJob = queryJob.waitFor();
    		  com.google.cloud.bigquery.QueryResponse response = bigquery.getQueryResults(jobId);
    		  bigquery.getTable("Agapes_Datasets", key+"_temp").delete();
    		 // bigquery.getTable("Agapes_Datasets", key+"_temp").delete();
			  //table.delete();


    		  } 
    		  
    		  }
    		  for ( String key : type.keySet() ) {
        		  if(key.equals("DMN")||key.equals("PLU")||key.equals("TVA")||key.equals("RGT")||key.equals("PMN")||key.equals("DMN")||key.equals("TOT")) {
        		  /////table final ////
        		  
        		  
        		  QueryJobConfiguration queryConfig =
        				    QueryJobConfiguration.newBuilder("insert into Agapes_Datasets.bigtable(Date,Restaurent,TPV,Tiroir,Ticket,Emp,Ligne,Heure,Type,Ligne_,PLU,PLU_code,Montant) select substr(Ligne, 1,11) as Date, substr(Ligne,12,4) as Restaurent, \r\n" + 
        				    		"       substr(Ligne,18,2) as TPV,substr(Ligne, 24,3) as Tiroir,\r\n" + 
        				    		"       substr(Ligne, 29,5) as Ticket, substr(Ligne, 35,3) as Emp, substr(Ligne, 39,5) as Ligne, \r\n" + 
        				    		"       substr(Ligne, 45,6) as Heure, substr(Ligne, 52,3) as Type, substr(Ligne, 56,39) as Ligne_ ,substr(Ligne, 97,4) as PLU,substr(Ligne, 102,5) as Code_PLU,substr(Ligne, 118,4) as Montant  from Agapes_Datasets."+key+"_temp")
        				        .setUseLegacySql(false)
        				        .build();
        		  
        		  JobId jobId = JobId.of(UUID.randomUUID().toString());
        		  com.google.cloud.bigquery.Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        		  
        		  queryJob = queryJob.waitFor();
        		  com.google.cloud.bigquery.QueryResponse response = bigquery.getQueryResults(jobId);
    			  //table.delete();
        		  bigquery.getTable("Agapes_Datasets", key+"_temp").delete();
        		  } 
        		  
        		  }
    	
        String[]recap = type.get("RECAP").split("ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ");	  
    	for (int s=2;s<recap.length;s++) {
    	  BlobId blobId = BlobId.of("agapes_storage", "Recap/"+s+".txt");
  		  BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
  		  Blob blob_act = storage.create(blobInfo, recap[s].toString().getBytes(UTF_8)); 
  		  // to do : la table à crée 
  		 
    	}
    
    	}
    	
    
    }
}
