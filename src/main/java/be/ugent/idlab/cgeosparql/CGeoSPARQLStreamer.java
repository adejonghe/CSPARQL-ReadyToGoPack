package be.ugent.idlab.cgeosparql;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.core.RDFDataset;
import com.github.jsonldjava.utils.JsonUtils;
import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.AnonId;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.rdf.model.impl.PropertyImpl;
import com.hp.hpl.jena.rdf.model.impl.ResourceImpl;

import eu.larkc.csparql.cep.api.RdfQuadruple;
import eu.larkc.csparql.cep.api.RdfStream;

public class CGeoSPARQLStreamer extends RdfStream implements Runnable {
	
	private static Logger logger = LoggerFactory.getLogger(CGeoSPARQLStreamer.class);

	private long sleepTime = 0;	
	private int runs = 0;
	private int currentRunNbr = 0;
	private Model event;
	private static final String EVENT_URL = "example_files/event.jsonld";
	private static final String SHIP_OBSERVATIONS = "example_files/shipObservations.txt";

	public CGeoSPARQLStreamer(String iri, long sleepTime, int runs) {
		super(iri);
		this.sleepTime = sleepTime;
		this.runs = runs;
		this.event = ModelFactory.createDefaultModel();
		
		try {
			String eventFile = new String(Files.readAllBytes(Paths.get(EVENT_URL)));
			event = deserializeJSON(eventFile, null);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	@Override
	public void run() {		
		
		currentRunNbr++;
		
		logger.info("Start run #" + currentRunNbr);
		
		try {
			
			BufferedReader reader = new BufferedReader(new FileReader(SHIP_OBSERVATIONS));
			String line = "";
			
			logger.info("Start Streaming...");
			
			while ((line = reader.readLine()) != null) {
				
				String[] coordinates = line.split(",");
				
				long timestamp = System.currentTimeMillis();
				
				StmtIterator it = event.listStatements();
	
				while (it.hasNext()) {
					Statement stmt = it.next();
					Triple t = stmt.asTriple();
					
					String s = replaceParameters(t.getSubject().toString(), timestamp, coordinates);
					String p = replaceParameters(t.getPredicate().toString(), timestamp, coordinates);
					String o = replaceParameters(t.getObject().toString(), timestamp, coordinates);
					
					RdfQuadruple q = new RdfQuadruple(s, p, o, timestamp);
					put(q);
				}
	
				Thread.sleep(sleepTime);
			}
			
			logger.info("Streaming ended ... No more data available");
			
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		if (currentRunNbr < runs) {
			run();
		}

	}	
	
	private static String replaceParameters(String txt, long timestamp, String[] coordinates) { 
		return txt.replaceAll("TST", String.valueOf(timestamp)).replaceAll("LON", coordinates[0]).replaceAll("LAT", coordinates[1]);
	}

	private static Model deserializeJSON(String asJsonSerialization, JsonLdOptions options) {

		Model model = ModelFactory.createDefaultModel();

		try {
			
			RDFDataset dataset = null;

			try {				
				Object jsonObject = JsonUtils.fromString(asJsonSerialization);

				if (options != null) {
					dataset = (RDFDataset) JsonLdProcessor.toRDF(jsonObject, options);
				} else {
					dataset = (RDFDataset) JsonLdProcessor.toRDF(jsonObject);
				}

			} catch (Exception e) {
				e.printStackTrace();
			}

			for (String graphName : dataset.graphNames()) {

				for (RDFDataset.Quad q : dataset.getQuads(graphName)) {
					
					Resource subject = null;					
					if (q.getSubject().isBlankNode()) {
						subject = new ResourceImpl(new AnonId(q.getSubject().getValue()));
					} else {
						subject = new ResourceImpl(q.getSubject().getValue());
					}

					Property predicate = new PropertyImpl(q.getPredicate().getValue());

					
					if (!q.getObject().isLiteral()) {
						
						Resource object = null;
						if (q.getObject().isBlankNode()) {
							object = new ResourceImpl(new AnonId(q.getObject().getValue()));
						} else {
							object = new ResourceImpl(q.getObject().getValue());
						}
						
						model.add(subject, predicate, object);
						
					} else {
						
						RDFDatatype type = NodeFactory.getType(q.getObject().getDatatype());
						Literal typedLiteral = ResourceFactory.createTypedLiteral(q.getObject().getValue(), type);
						
						model.add(subject, predicate, typedLiteral);
					}
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return model;
	}

}
