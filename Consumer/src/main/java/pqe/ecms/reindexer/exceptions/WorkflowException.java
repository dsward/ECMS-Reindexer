package pqe.ecms.reindexer.exceptions;

import com.amazonaws.AmazonClientException;
import com.proquest.editorial.commons.parse.ParseException;
import pqe.ecms.search.exceptions.MapperException;
import pqe.ecms.search.exceptions.UnknownMapperException;

import java.io.IOException;
import java.sql.SQLException;

public class WorkflowException extends Throwable {
	public WorkflowException(SQLException e) {
		super(e);
	}

	public WorkflowException(AmazonClientException e) {
		super(e);
	}

	public WorkflowException(ParseException e) {
		super(e);
	}

	public WorkflowException(UnknownMapperException e) {
		super(e);
	}

	public WorkflowException(MapperException e) {
		super(e);
	}

	public WorkflowException(IOException e) {
		super(e);
	}

	public WorkflowException() {

	}
}
