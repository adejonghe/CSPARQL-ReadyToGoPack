package be.ugent.idlab.cgeosparql;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.larkc.csparql.common.utils.CsparqlUtils;
import eu.larkc.csparql.common.utils.ReasonerChainingType;
import eu.larkc.csparql.core.engine.ConsoleFormatter;
import eu.larkc.csparql.core.engine.CsparqlEngineImpl;
import eu.larkc.csparql.core.engine.CsparqlQueryResultProxy;

public class CGeoSPARQLTester {

	private static Logger logger = LoggerFactory.getLogger(CGeoSPARQLTester.class);
	
	private static final HashMap<String, String[]> QUERIES;
	
	static {		
		QUERIES = new HashMap<String, String[]>();
		
		QUERIES.put("allShips", new String[]{});
		
		QUERIES.put("specificShips", new String[]{});
		
		QUERIES.put("shipsInSifferdok", new String[]{
				"dokken.rdf"
		});
		
		QUERIES.put("featuresWithIncident", new String[]{
				"incidents_cs.rdf",
				"incidents_wt.rdf",
				"bedrijfspercelenhavengent.rdf",
				"windturbineshavengent.rdf"
		});
		
		QUERIES.put("shipsToEvacuate", new String[]{
				"incidents_wt.rdf",
				"windturbineshavengent.rdf"
		});
	}
	
	private static final String TBOX = "example_files/caprads.owl";
	
	private static final String TEST_QUERY = "allShips";

	public static void main(String[] args) {

		try {
			
			// Configure log4j logger for the csparql engine
			PropertyConfigurator.configure("log4j_configuration/csparql_readyToGoPack_log4j.properties");
			
			String queryName = TEST_QUERY;
			
			if (args.length > 0) { 
				queryName = args[0];
			}
			
			logger.info("Running with query: " + queryName);
			
			// Initialize engine
			CsparqlEngineImpl engine = new CsparqlEngineImpl();
			engine.initialize(true);							
			
			// Register new streams in the engine
			CGeoSPARQLStreamer stream = new CGeoSPARQLStreamer("http://cgeosparql.idlab.ugent.be/caprads/sgraph", 3000L);
			engine.registerStream(stream);

			//Register static knowledge
			for (String filename : QUERIES.get(queryName)) {
				engine.putStaticNamedModel("http://cgeosparql.idlab.ugent.be/caprads/" + filename, CsparqlUtils.serializeRDFFile("example_files/static/" + filename));
			}

			// Register new query in the engine					
			String query = readFileContent("example_files/queries/"+queryName+".txt");
			CsparqlQueryResultProxy c = engine.registerQuery("REGISTER STREAM " + queryName +" AS " + query, false);

			// Register observer
			c.addObserver(new ConsoleFormatter());
			
			// Assign reasoner to this the query
			String tBox = readFileContent(TBOX);
			engine.updateReasoner(c.getSparqlQueryId(), CsparqlUtils.fileToString("example_files/rdfs.rules"), ReasonerChainingType.FORWARD, tBox);
	
			// Start streaming
			new Thread(stream).start();

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}
	
	public static String readFileContent(String url) {
		
		String fileContent = "";
		try {
			fileContent = new String(Files.readAllBytes(Paths.get(url)));
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
		
		return fileContent;			
	}
}
