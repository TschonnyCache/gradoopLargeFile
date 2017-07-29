package github.TschonnyCache.gradoopLargeFile;

import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.rdf.RDFDataSource;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import org.apache.flink.api.java.ExecutionEnvironment;

public class gradoopLargeFile {

	public static void main(String[] args) throws Exception {

		if (args[0] == null || args[0].trim().isEmpty()) {
	        System.out.println("You need to specify a path!");
	        return;
	    } else {
	        
	    	ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
	    	GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
	    	
	    	String nTripleFile = args[0];
	        DataSource dataSource = new RDFDataSource(nTripleFile, config);
	        LogicalGraph graph = dataSource.getLogicalGraph();
	        long count = graph.getVertices().count();
	        System.out.println(count);
	    }
	}
}
