package pqe.ecms.reindexer.exceptions;

/**
 * Exception thrown in the cases where a client could not be properly initialized.
 * 
 * @author fperez
 *
 */
public class ClientInstantiationException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public ClientInstantiationException() {
		super();
	}
	
	public ClientInstantiationException(final String message) {
		super(message);
	}
	
	public ClientInstantiationException(final String message, final Throwable error){
		super(message,error);
	}
}
