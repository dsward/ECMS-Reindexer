package pqe.ecms.reindexer.workflow;

import com.proquest.editorial.commons.parse.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pqe.ecms.client.EcmsClientFactory;
import pqe.ecms.reindexer.exceptions.WorkflowException;
import pqe.ecms.search.exceptions.MapperException;
import pqe.ecms.search.exceptions.UnknownMapperException;
import pqe.ecms.search.map.ecmsindex.EcmsField;
import pqe.ecms.search.map.ecmsindex.EcmsMapper;

import java.util.Arrays;

public class DocumentMapper {

	private static final Logger LOGGER = LoggerFactory.getLogger(DocumentMapper.class);

	public IndexingDocument apply(IndexingDocument document) throws WorkflowException {

		try {
			EcmsMapper ecmsMapper = EcmsMapper.getInstance();
			ecmsMapper.initDocument(document.getDocument(), Arrays.asList(EcmsField.values()), EcmsClientFactory.getInstance());

			ecmsMapper.updateDocumentId(document.getDocumentId());
			document.setMappedDocument(ecmsMapper.mapAllFields());

			return document;

		} catch (ParseException e) {
			LOGGER.error("Exception", e);
			throw new WorkflowException(e);

		} catch (UnknownMapperException e) {
			LOGGER.error("Exception", e);
			throw new WorkflowException(e);

		} catch (MapperException e) {
			LOGGER.error("Exception", e);
			throw new WorkflowException(e);
		}

	}
}
