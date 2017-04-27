package be.ugent.idlab.cgeosparql;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

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

	private long sleepTime;	
	private Model event;
	private static final String EVENT_URL = "example_files/event.jsonld";

	public CGeoSPARQLStreamer(String iri, long sleepTime) {
		super(iri);
		this.sleepTime = sleepTime;
		this.event = ModelFactory.createDefaultModel();
		
		try {
			String eventFile = new String(Files.readAllBytes(Paths.get(EVENT_URL)));
			event = deserializeJSON(eventFile, null);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	public void run() {

		while (true) {
			
			try {

				long timestamp = System.currentTimeMillis();

				StmtIterator it = event.listStatements();

				while (it.hasNext()) {
					Statement stmt = it.next();
					Triple t = stmt.asTriple();
					
					String s = t.getSubject().toString().replaceAll("XYZ", String.valueOf(timestamp));
					String p = t.getPredicate().toString().replaceAll("XYZ", String.valueOf(timestamp));
					String o = t.getObject().toString().replaceAll("XYZ", String.valueOf(timestamp));
					
					RdfQuadruple q = new RdfQuadruple(s, p, o, timestamp);
					put(q);
				}

				Thread.sleep(sleepTime);

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

	public static Model deserializeJSON(String asJsonSerialization, JsonLdOptions options) {

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
